package auditlog

import (
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	pb "otlp"
	v1 "otlp/collector/logs/v1"
	common "otlp/common/v1"
	logs "otlp/logs/v1"
	resource "otlp/resource/v1"
	"strconv"

	"container/list"
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	MaxMemTableSize   = 500
	BatchSize         = 50
	MaxBatchSize      = 150
	ConnectExpireTime = 10
)

var AttrName = [11]string{"time", "ip-addr", "host-name", "op-name", "src-path", "dst-path", "error", "request-id", "latency", "src-inode", "dst-inode"}

var ga *AuditLog = nil
var once sync.Once

// Table
type Table struct {
	records []*logs.LogRecord
	tail    int
}

func NewTable() *Table {
	return &Table{
		records: make([]*logs.LogRecord, MaxMemTableSize),
		tail:    0,
	}
}

func (table *Table) addRecord(logRecord *logs.LogRecord) {
	table.records[table.tail] = logRecord
	table.tail += 1
}

func (table *Table) Size() int {
	return table.tail
}

func (table *Table) Clear() {
	for i := 0; i < MaxMemTableSize; i++ {
		table.records[i] = nil
	}
	table.tail = 0
}

type TablePool struct {
	p *sync.Pool
}

func (tablePool *TablePool) Get() *Table {
	return tablePool.p.Get().(*Table)
}

func (tablePool *TablePool) Put(x interface{}) {
	tablePool.p.Put(x)
}

func NewTablePool() *TablePool {
	return &TablePool{
		p: &sync.Pool{
			New: func() interface{} {
				return NewTable()
			},
		},
	}
}

type ConnObject struct {
	c          *grpc.ClientConn
	lastActive int64
}

type ConnPool struct {
	conns  chan *ConnObject
	mincap int
	maxcap int
	expire int64
	target string
}

func NewConnPool(target string, mincap, maxcap int, expire int64) *ConnPool {
	p := &ConnPool{
		conns:  make(chan *ConnObject, maxcap),
		mincap: mincap,
		maxcap: maxcap,
		expire: expire,
		target: target,
	}
	return p
}

func (connPool *ConnPool) Get() (c *grpc.ClientConn, err error) {
	var o *ConnObject
	for {
		select {
		case o = <-connPool.conns:
		default:
			return connPool.NewConnect(connPool.target)
		}
		if time.Now().UnixNano()-o.lastActive > connPool.expire {
			_ = o.c.Close()
			o = nil
			continue
		}
		return o.c, nil
	}
}

func (connPool *ConnPool) NewConnect(target string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	return conn, err
}

func (connPool *ConnPool) Put(c *grpc.ClientConn) {
	o := &ConnObject{
		c:          c,
		lastActive: time.Now().UnixNano(),
	}
	select {
	case connPool.conns <- o:
		return
	default:
		if o.c != nil {
			o.c.Close()
		}
		return
	}
}

type AuditLog struct {
	volName         string
	hostName        string
	ipAddr          string
	appkey          string
	memTable        *Table
	freezeTableList *list.List
	tablePool       *TablePool
	connPool        *ConnPool
	enqueueC        chan *logs.LogRecord
	stopC           chan struct{}
	freezeLock      sync.Mutex
}

func getAddr() (HostName, IPAddr string) {
	hostName, err := os.Hostname()
	if err != nil {
		HostName = "Unknown"
		log.LogWarnf("Get host name failed, replaced by unknown. err(%v)", err)
	} else {
		HostName = hostName
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		IPAddr = "Unknown"
		log.LogWarnf("Get ip address failed, replaced by unknown. err(%v)", err)
	} else {
		var ip_addrs []string
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				ip_addrs = append(ip_addrs, ipnet.IP.String())
			}
		}
		if len(ip_addrs) > 0 {
			IPAddr = strings.Join(ip_addrs, ",")
		} else {
			IPAddr = "Unknown"
			log.LogWarnf("Get ip address failed, replaced by unknown. err(%v)", err)
		}
	}
	return
}

func InitAudit(vol, addr, key string) (*AuditLog, error) {
	host, ip := getAddr()
	a := &AuditLog{
		volName:         vol,
		hostName:        host,
		ipAddr:          ip,
		appkey:          key,
		memTable:        NewTable(),
		freezeTableList: list.New(),
		enqueueC:        make(chan *logs.LogRecord, 5000),
		//dequeueC:        make(chan struct{}, 1024),
		//freezeC:         make(chan struct{}),
		stopC: make(chan struct{}),
	}
	a.tablePool = NewTablePool()
	expireTime := int64(time.Second * ConnectExpireTime)
	a.connPool = NewConnPool(addr, 1, 5, expireTime)
	ga = a
	go ga.logReporter()
	go ga.logCollector()
	return ga, nil
}

// logCollector put one log record to mem-table or
// freeze mem-table to frozen-table.
func (audit *AuditLog) logCollector() {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			audit.tryFreezeTable()
		case logRecored := <-audit.enqueueC:
			if audit.memTable.Size() >= MaxMemTableSize {
				audit.tryFreezeTable()
			}
			audit.memTable.addRecord(logRecored)
		case <-audit.stopC:
			log.LogDebugf("logCollector stopC closed.")
			ticker.Stop()
			return
		}
	}
}

// logReporter push one frozen-table to LogHouse or
// ask logCollector to freeze the mem-table.
func (audit *AuditLog) logReporter() {
	for {
		select {
		case <-audit.stopC:
			log.LogDebugf("logReporter stopC closed.")
			return
		default:
			isEmpty := audit.tryPushFreezeTable()
			if isEmpty {
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func AddEntry(op, src, dst string, err error, latency int64, srcInode, dstInode uint64) {
	if ga == nil {
		return
	}
	var errStr string
	if err != nil {
		errStr = err.Error()
	} else {
		errStr = "nil"
	}
	curTime := time.Now()
	curTimeStr := curTime.Format("2006-01-02 15:04:05")
	requestId := uuid.New().String()
	latencyStr := strconv.FormatInt(latency, 10) + " us"
	srcInodeStr := strconv.FormatUint(srcInode, 10)
	dstInodeStr := strconv.FormatUint(dstInode, 10)
	ga.addEntry(curTime, curTimeStr, ga.ipAddr, ga.hostName, op, src, dst, errStr, requestId, latencyStr, srcInodeStr, dstInodeStr)
}

func (audit *AuditLog) addEntry(curTime time.Time, args ...interface{}) {
	var attrs []common.KeyValue
	for idx, arg := range args {
		switch arg.(type) {
		case string:
			attrs = append(attrs, common.KeyValue{
				Key: AttrName[idx],
				Value: common.AnyValue{
					Value: &common.AnyValue_StringValue{
						StringValue: arg.(string),
					},
				},
			})
		default:
			attrs = append(attrs, common.KeyValue{
				Key: AttrName[idx],
				Value: common.AnyValue{
					Value: &common.AnyValue_StringValue{
						StringValue: "unknown-value",
					},
				},
			})
		}
	}
	logRecord := &logs.LogRecord{
		TimeUnixNano: uint64(curTime.UnixNano()),
		Attributes:   attrs,
		TraceId:      pb.NewTraceID([16]byte{'t', 'r', 'a', 'c', 'e', 'i', 'd'}),
		SpanId:       pb.NewSpanID([8]byte{'s', 'p', 'a', 'n', 'i', 'd'}),
	}
	audit.enqueueC <- logRecord
}

func (audit *AuditLog) tryFreezeTable() {
	freezingTable := audit.memTable
	if freezingTable.Size() > 0 {
		audit.memTable = audit.tablePool.Get()
		// todo : non-block list
		audit.freezeLock.Lock()
		audit.freezeTableList.PushBack(freezingTable)
		audit.freezeLock.Unlock()
	}
}

func (audit *AuditLog) tryPushFreezeTable() (isEmpty bool) {
	audit.freezeLock.Lock()
	// export frozen table list size to prometheus
	auditSizeExporter := exporter.NewGauge("AuditFreezeTableListSize")
	auditSizeExporter.SetWithLabels(float64(audit.freezeTableList.Len()), map[string]string{exporter.Vol: audit.volName})
	element := audit.freezeTableList.Front()
	if element == nil {
		audit.freezeLock.Unlock()
		return true
	}
	audit.freezeTableList.Remove(element)
	audit.freezeLock.Unlock()

	table := element.Value.(*Table)
	remain := table.Size()
	from := 0
	step := 0
	conn, _ := audit.connPool.Get()
	cli := v1.NewLogsServiceClient(conn)
	for remain > 0 {
		startTime := time.Now()
		if remain >= BatchSize {
			step = BatchSize
		} else {
			step = remain
		}
		logRecords := table.records[from : from+step]
		req := genRequest(audit.appkey, logRecords)
		_, err := cli.Export(context.TODO(), &req)
		if err != nil {
			log.LogErrorf("Export to gRPC server failed. error: %v", err)
		}
		log.LogDebugf("Export to gRPC server success. from: %v step: %v to: %v", from, step, from+step)
		from = from + step
		remain = remain - step
		// export latency to prometheus
		auditLatencyExporter := exporter.NewGauge("AuditExportLatency")
		auditLatencyExporter.SetWithLabels(float64(time.Since(startTime).Milliseconds()), map[string]string{exporter.Vol: audit.volName})
	}
	table.Clear()
	audit.tablePool.Put(table)
	audit.connPool.Put(conn)
	return false
}

func StopAudit() {
	if ga == nil {
		return
	}
	once.Do(ga.stop)
}

func (audit *AuditLog) stop() {
	close(audit.stopC)
	if audit.memTable.Size() > 0 {
		audit.freezeTableList.PushBack(audit.memTable)
	}
	var wg sync.WaitGroup
	for element := audit.freezeTableList.Front(); element != nil; element = audit.freezeTableList.Front() {
		audit.freezeTableList.Remove(element)
		table := element.Value.(*Table)
		wg.Add(1)
		go func(wg *sync.WaitGroup, table *Table) {
			conn, _ := audit.connPool.Get()
			cli := v1.NewLogsServiceClient(conn)
			remain := table.Size()
			from := 0
			step := 0
			for remain > 0 {
				if remain >= MaxBatchSize {
					step = MaxBatchSize
				} else {
					step = remain
				}
				logRecords := table.records[from : from+step]
				req := genRequest(audit.appkey, logRecords)
				_, err := cli.Export(context.TODO(), &req)
				if err != nil {
					log.LogErrorf("Export to gRPC server failed, err: %v", err)
				}
				from = from + step
				remain = remain - step
			}
			wg.Done()
		}(&wg, table)
	}
	wg.Wait()
}

func genRequest(appkey string, logRecords []*logs.LogRecord) v1.ExportLogsServiceRequest {
	req := v1.ExportLogsServiceRequest{
		ResourceLogs: []*logs.ResourceLogs{
			{
				Resource: resource.Resource{
					Attributes: []common.KeyValue{
						{
							Key: "app.key",
							Value: common.AnyValue{
								Value: &common.AnyValue_StringValue{
									StringValue: appkey,
								},
							},
						},
					},
				},
				InstrumentationLibraryLogs: []*logs.InstrumentationLibraryLogs{
					{
						InstrumentationLibrary: common.InstrumentationLibrary{
							Name:    "Cfs-Client-Audit",
							Version: "1.0-beta",
						},
						Logs:      logRecords,
						SchemaUrl: "url",
					},
				},
				SchemaUrl: "url",
			},
		},
	}
	return req
}
