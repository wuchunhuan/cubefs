// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

//metrics
const (
	StatPeriod                 = time.Minute * time.Duration(1)
	MetricDataNodesUsedGB      = "dataNodes_used_GB"
	MetricDataNodesTotalGB     = "dataNodes_total_GB"
	MetricDataNodesIncreasedGB = "dataNodes_increased_GB"
	MetricMetaNodesUsedGB      = "metaNodes_used_GB"
	MetricMetaNodesTotalGB     = "metaNodes_total_GB"
	MetricMetaNodesIncreasedGB = "metaNodes_increased_GB"
	MetricDataNodesCount       = "dataNodes_count"
	MetricMetaNodesCount       = "metaNodes_count"
	MetricVolCount             = "vol_count"
	MetricVolTotalGB           = "vol_total_GB"
	MetricVolUsedGB            = "vol_used_GB"
	MetricVolUsageGB           = "vol_usage_ratio"
	MetricVolMetaCount         = "vol_meta_count"
	MetricDiskError            = "disk_error"
	MetricDataNodesInactive    = "dataNodes_inactive"
	MetricInactiveDataNodeInfo = "inactive_dataNodes_info"
	MetricMetaNodesInactive    = "metaNodes_inactive"
	MetricInactiveMataNodeInfo = "inactive_mataNodes_info"
	MetricMetaInconsistent     = "mp_inconsistent"
	MetricMasterNoLeader       = "master_no_leader"
	MetricMasterNoCache        = "master_no_cache"
	MetricMasterSnapshot       = "master_snapshot"

	MetricMissingDp  = "missing_dp"
	MetricDpNoLeader = "dp_no_leader"
	MetricMissingMp  = "missing_mp"
	MetricMpNoLeader = "mp_no_leader"
)

var WarnMetrics *warningMetrics

type monitorMetrics struct {
	cluster              *Cluster
	dataNodesCount       *exporter.Gauge
	metaNodesCount       *exporter.Gauge
	volCount             *exporter.Gauge
	dataNodesTotal       *exporter.Gauge
	dataNodesUsed        *exporter.Gauge
	dataNodeIncreased    *exporter.Gauge
	metaNodesTotal       *exporter.Gauge
	metaNodesUsed        *exporter.Gauge
	metaNodesIncreased   *exporter.Gauge
	volTotalSpace        *exporter.GaugeVec
	volUsedSpace         *exporter.GaugeVec
	volUsage             *exporter.GaugeVec
	volMetaCount         *exporter.GaugeVec
	diskError            *exporter.GaugeVec
	dataNodesInactive    *exporter.Gauge
	InactiveDataNodeInfo *exporter.GaugeVec
	metaNodesInactive    *exporter.Gauge
	InactiveMataNodeInfo *exporter.GaugeVec
	metaEqualCheckFail   *exporter.GaugeVec
	masterNoLeader       *exporter.Gauge
	masterNoCache        *exporter.GaugeVec
	masterSnapshot       *exporter.Gauge

	volNames        map[string]struct{}
	badDisks        map[string]string
	inconsistentMps map[string]string
}

func newMonitorMetrics(c *Cluster) *monitorMetrics {
	return &monitorMetrics{cluster: c,
		volNames:        make(map[string]struct{}),
		badDisks:        make(map[string]string),
		inconsistentMps: make(map[string]string),
	}
}

type warningMetrics struct {
	cluster        *Cluster
	missingDp      *exporter.GaugeVec
	dpNoLeader     *exporter.GaugeVec
	missingMp      *exporter.GaugeVec
	mpNoLeader     *exporter.GaugeVec
	dpMutex        sync.Mutex
	mpMutex        sync.Mutex
	dpNoLeaderInfo map[uint64]int64
	mpNoLeaderInfo map[uint64]int64
}

func newWarningMetrics(c *Cluster) *warningMetrics {
	return &warningMetrics{
		cluster:        c,
		missingDp:      exporter.NewGaugeVec(MetricMissingDp, "", []string{"clusterName", "partitionID", "addr"}),
		dpNoLeader:     exporter.NewGaugeVec(MetricDpNoLeader, "", []string{"clusterName", "partitionID"}),
		missingMp:      exporter.NewGaugeVec(MetricMissingMp, "", []string{"clusterName", "partitionID", "addr"}),
		mpNoLeader:     exporter.NewGaugeVec(MetricMpNoLeader, "", []string{"clusterName", "partitionID"}),
		dpNoLeaderInfo: make(map[uint64]int64),
		mpNoLeaderInfo: make(map[uint64]int64),
	}
}

func (m *warningMetrics) reset() {
	m.dpMutex.Lock()
	for dp := range m.dpNoLeaderInfo {
		m.dpNoLeader.DeleteLabelValues(m.cluster.Name, strconv.FormatUint(dp, 10))
		delete(m.dpNoLeaderInfo, dp)
	}
	m.dpMutex.Unlock()

	m.mpMutex.Lock()
	for mp := range m.mpNoLeaderInfo {
		m.mpNoLeader.DeleteLabelValues(m.cluster.Name, strconv.FormatUint(mp, 10))
		delete(m.mpNoLeaderInfo, mp)
	}
	m.mpMutex.Unlock()
}

//leader only
func (m *warningMetrics) WarnMissingDp(clusterName, addr string, partitionID uint64) {
	if clusterName != m.cluster.Name {
		return
	}
	m.missingDp.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10), addr)
}

//func (m *warningMetrics) WarnDpNoLeader(clusterName string, partitionID uint64) {
//	m.dpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
//}

//leader only
func (m *warningMetrics) WarnDpNoLeader(clusterName string, partitionID uint64, report bool) {
	if clusterName != m.cluster.Name {
		return
	}

	m.dpMutex.Lock()
	defer m.dpMutex.Unlock()
	t, ok := m.dpNoLeaderInfo[partitionID]
	if !report {
		if ok {
			delete(m.dpNoLeaderInfo, partitionID)
			m.dpNoLeader.DeleteLabelValues(clusterName, strconv.FormatUint(partitionID, 10))
		}
		return
	}

	now := time.Now().Unix()
	if !ok {
		m.dpNoLeaderInfo[partitionID] = now
		return
	}
	if now-t > m.cluster.cfg.NoLeaderReportInterval {
		m.dpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
		m.dpNoLeaderInfo[partitionID] = now
	}
}

//leader only
func (m *warningMetrics) WarnMissingMp(clusterName, addr string, partitionID uint64) {
	if clusterName != m.cluster.Name {
		return
	}
	m.missingMp.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10), addr)
}

//func (m *warningMetrics) WarnMpNoLeader(clusterName string, partitionID uint64) {
//	m.mpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
//}

//leader only
func (m *warningMetrics) WarnMpNoLeader(clusterName string, partitionID uint64, report bool) {
	if clusterName != m.cluster.Name {
		return
	}
	m.mpMutex.Lock()
	defer m.mpMutex.Unlock()
	t, ok := m.mpNoLeaderInfo[partitionID]
	if !report {
		if ok {
			delete(m.mpNoLeaderInfo, partitionID)
			m.mpNoLeader.DeleteLabelValues(clusterName, strconv.FormatUint(partitionID, 10))
		}
		return
	}

	now := time.Now().Unix()

	if !ok {
		m.mpNoLeaderInfo[partitionID] = now
		return
	}

	if now-t > m.cluster.cfg.NoLeaderReportInterval {
		m.mpNoLeader.SetWithLabelValues(1, clusterName, strconv.FormatUint(partitionID, 10))
		m.mpNoLeaderInfo[partitionID] = now
	}

}

func (mm *monitorMetrics) start() {
	mm.dataNodesTotal = exporter.NewGauge(MetricDataNodesTotalGB)
	mm.dataNodesUsed = exporter.NewGauge(MetricDataNodesUsedGB)
	mm.dataNodeIncreased = exporter.NewGauge(MetricDataNodesIncreasedGB)
	mm.metaNodesTotal = exporter.NewGauge(MetricMetaNodesTotalGB)
	mm.metaNodesUsed = exporter.NewGauge(MetricMetaNodesUsedGB)
	mm.metaNodesIncreased = exporter.NewGauge(MetricMetaNodesIncreasedGB)
	mm.dataNodesCount = exporter.NewGauge(MetricDataNodesCount)
	mm.metaNodesCount = exporter.NewGauge(MetricMetaNodesCount)
	mm.volCount = exporter.NewGauge(MetricVolCount)
	mm.volTotalSpace = exporter.NewGaugeVec(MetricVolTotalGB, "", []string{"volName"})
	mm.volUsedSpace = exporter.NewGaugeVec(MetricVolUsedGB, "", []string{"volName"})
	mm.volUsage = exporter.NewGaugeVec(MetricVolUsageGB, "", []string{"volName"})
	mm.volMetaCount = exporter.NewGaugeVec(MetricVolMetaCount, "", []string{"volName", "type"})
	mm.diskError = exporter.NewGaugeVec(MetricDiskError, "", []string{"addr", "path"})
	mm.dataNodesInactive = exporter.NewGauge(MetricDataNodesInactive)
	mm.InactiveDataNodeInfo = exporter.NewGaugeVec(MetricInactiveDataNodeInfo, "", []string{"clusterName", "addr"})
	mm.metaNodesInactive = exporter.NewGauge(MetricMetaNodesInactive)
	mm.InactiveMataNodeInfo = exporter.NewGaugeVec(MetricInactiveMataNodeInfo, "", []string{"clusterName", "addr"})
	mm.metaEqualCheckFail = exporter.NewGaugeVec(MetricMetaInconsistent, "", []string{"volume", "mpId"})

	mm.masterSnapshot = exporter.NewGauge(MetricMasterSnapshot)
	mm.masterNoLeader = exporter.NewGauge(MetricMasterNoLeader)
	mm.masterNoCache = exporter.NewGaugeVec(MetricMasterNoCache, "", []string{"volName"})
	go mm.statMetrics()
}

func (mm *monitorMetrics) statMetrics() {
	ticker := time.NewTicker(StatPeriod)
	defer func() {
		if err := recover(); err != nil {
			ticker.Stop()
			log.LogErrorf("statMetrics panic,msg:%v", err)
		}
	}()

	for {
		select {
		case <-ticker.C:
			partition := mm.cluster.partition
			if partition != nil && partition.IsRaftLeader() {
				mm.resetFollowerMetrics()
				mm.doStat()
			} else {
				mm.resetAllLeaderMetrics()
				mm.doFollowerStat()
			}
		}
	}
}

func (mm *monitorMetrics) doFollowerStat() {
	if mm.cluster.leaderInfo.addr == "" {
		mm.masterNoLeader.Set(1)
	} else {
		mm.masterNoLeader.Set(0)
	}
	if mm.cluster.fsm.onSnapshot {
		mm.masterSnapshot.Set(1)
	} else {
		mm.masterSnapshot.Set(0)
	}
	mm.setVolNoCacheMetrics()
}

func (mm *monitorMetrics) doStat() {
	dataNodeCount := mm.cluster.dataNodeCount()
	mm.dataNodesCount.Set(float64(dataNodeCount))
	metaNodeCount := mm.cluster.metaNodeCount()
	mm.metaNodesCount.Set(float64(metaNodeCount))
	volCount := len(mm.cluster.vols)
	mm.volCount.Set(float64(volCount))
	mm.dataNodesTotal.Set(float64(mm.cluster.dataNodeStatInfo.TotalGB))
	mm.dataNodesUsed.Set(float64(mm.cluster.dataNodeStatInfo.UsedGB))
	mm.dataNodeIncreased.Set(float64(mm.cluster.dataNodeStatInfo.IncreasedGB))
	mm.metaNodesTotal.Set(float64(mm.cluster.metaNodeStatInfo.TotalGB))
	mm.metaNodesUsed.Set(float64(mm.cluster.metaNodeStatInfo.UsedGB))
	mm.metaNodesIncreased.Set(float64(mm.cluster.metaNodeStatInfo.IncreasedGB))
	mm.setVolMetrics()
	mm.setDiskErrorMetric()
	mm.setInactiveDataNodesCount()
	mm.setInactiveMetaNodesCount()
	mm.setMpInconsistentErrorMetric()
}

func (mm *monitorMetrics) setVolNoCacheMetrics() {
	deleteVolNames := make(map[string]struct{})
	mm.cluster.followerReadManager.rwMutex.RLock()
	defer mm.cluster.followerReadManager.rwMutex.RUnlock()

	for volName, stat := range mm.cluster.followerReadManager.status {
		if stat == true {
			deleteVolNames[volName] = struct{}{}
			log.LogDebugf("setVolNoCacheMetrics deleteVolNames volName %v", volName)
			continue
		}
		log.LogWarnf("setVolNoCacheMetrics volName %v", volName)
		mm.masterNoCache.SetWithLabelValues(1, volName)
	}

	for volName := range deleteVolNames {
		mm.masterNoCache.DeleteLabelValues(volName)
	}
}

func (mm *monitorMetrics) setVolMetrics() {
	deleteVolNames := make(map[string]struct{})
	for k, v := range mm.volNames {
		deleteVolNames[k] = v
		delete(mm.volNames, k)
	}

	mm.cluster.volStatInfo.Range(func(key, value interface{}) bool {
		volStatInfo, ok := value.(*volStatInfo)
		if !ok {
			return true
		}
		volName, ok := key.(string)
		if !ok {
			return true
		}
		mm.volNames[volName] = struct{}{}
		if _, ok := deleteVolNames[volName]; ok {
			delete(deleteVolNames, volName)
		}

		mm.volTotalSpace.SetWithLabelValues(float64(volStatInfo.TotalSize), volName)
		mm.volUsedSpace.SetWithLabelValues(float64(volStatInfo.UsedSize), volName)
		usedRatio, e := strconv.ParseFloat(volStatInfo.UsedRatio, 64)
		if e == nil {
			mm.volUsage.SetWithLabelValues(usedRatio, volName)
		}
		if usedRatio > volWarnUsedRatio {
			WarnBySpecialKey("vol size used too high", fmt.Sprintf("vol: %v(total: %v, used: %v) has used(%v) to be full", volName, volStatInfo.TotalSize, volStatInfo.UsedRatio, volStatInfo.UsedSize))
		}

		return true
	})

	for volName, vol := range mm.cluster.allVols() {
		inodeCount := uint64(0)
		dentryCount := uint64(0)
		mpCount := uint64(0)
		for _, mpv := range vol.getMetaPartitionsView() {
			inodeCount += mpv.InodeCount
			dentryCount += mpv.DentryCount
			mpCount += 1
		}
		mm.volMetaCount.SetWithLabelValues(float64(inodeCount), volName, "inode")
		mm.volMetaCount.SetWithLabelValues(float64(dentryCount), volName, "dentry")
		mm.volMetaCount.SetWithLabelValues(float64(mpCount), volName, "mp")
		mm.volMetaCount.SetWithLabelValues(float64(vol.getDataPartitionsCount()), volName, "dp")
	}

	for volName := range deleteVolNames {
		mm.deleteVolMetric(volName)
	}
}

func (mm *monitorMetrics) deleteVolMetric(volName string) {
	mm.volTotalSpace.DeleteLabelValues(volName)
	mm.volUsedSpace.DeleteLabelValues(volName)
	mm.volUsage.DeleteLabelValues(volName)
	mm.volMetaCount.DeleteLabelValues(volName, "inode")
	mm.volMetaCount.DeleteLabelValues(volName, "dentry")
	mm.volMetaCount.DeleteLabelValues(volName, "mp")
	mm.volMetaCount.DeleteLabelValues(volName, "dp")
}
func (mm *monitorMetrics) setMpInconsistentErrorMetric() {
	deleteMps := make(map[string]string)
	for k, v := range mm.inconsistentMps {
		deleteMps[k] = v
		delete(mm.inconsistentMps, k)
	}
	mm.cluster.volMutex.RLock()
	defer mm.cluster.volMutex.RUnlock()

	for _, vol := range mm.cluster.vols {
		for _, mp := range vol.MetaPartitions {
			if mp.IsRecover || mp.EqualCheckPass {
				continue
			}
			idStr := strconv.FormatUint(mp.PartitionID, 10)
			mm.metaEqualCheckFail.SetWithLabelValues(1, vol.Name, idStr)
			mm.inconsistentMps[idStr] = vol.Name
			log.LogWarnf("setMpInconsistentErrorMetric.mp %v SetWithLabelValues id %v vol %v", mp.PartitionID, idStr, vol.Name)
			delete(deleteMps, idStr)
		}
	}

	for k, v := range deleteMps {
		mm.metaEqualCheckFail.DeleteLabelValues(v, k)
	}
}

func (mm *monitorMetrics) setDiskErrorMetric() {
	deleteBadDisks := make(map[string]string)
	for k, v := range mm.badDisks {
		deleteBadDisks[k] = v
		delete(mm.badDisks, k)
	}
	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		for _, badDisk := range dataNode.BadDisks {
			for _, partition := range dataNode.DataPartitionReports {
				if partition.DiskPath == badDisk {
					mm.diskError.SetWithLabelValues(1, dataNode.Addr, badDisk)
					mm.badDisks[badDisk] = dataNode.Addr
					delete(deleteBadDisks, badDisk)
					break
				}
			}
		}

		return true
	})

	for k, v := range deleteBadDisks {
		mm.diskError.DeleteLabelValues(v, k)
	}
}

func (mm *monitorMetrics) setInactiveMetaNodesCount() {
	var inactiveMetaNodesCount int64
	mm.cluster.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode, ok := node.(*MetaNode)
		if !ok {
			return true
		}
		if !metaNode.IsActive {
			inactiveMetaNodesCount++
			mm.InactiveMataNodeInfo.SetWithLabelValues(1, mm.cluster.Name, metaNode.Addr)
		} else {
			mm.InactiveMataNodeInfo.DeleteLabelValues(mm.cluster.Name, metaNode.Addr)
		}
		return true
	})
	mm.metaNodesInactive.Set(float64(inactiveMetaNodesCount))
}

func (mm *monitorMetrics) setInactiveDataNodesCount() {
	var inactiveDataNodesCount int64
	mm.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		if !dataNode.isActive {
			inactiveDataNodesCount++
			mm.InactiveDataNodeInfo.SetWithLabelValues(1, mm.cluster.Name, dataNode.Addr)
		} else {
			mm.InactiveDataNodeInfo.DeleteLabelValues(mm.cluster.Name, dataNode.Addr)
		}
		return true
	})
	mm.dataNodesInactive.Set(float64(inactiveDataNodesCount))
}

func (mm *monitorMetrics) clearInconsistentMps() {
	for k := range mm.inconsistentMps {
		mm.metaEqualCheckFail.DeleteLabelValues(k)
	}
}

func (mm *monitorMetrics) clearVolMetrics() {
	mm.cluster.volStatInfo.Range(func(key, value interface{}) bool {
		if volName, ok := key.(string); ok {
			mm.deleteVolMetric(volName)
		}
		return true
	})
}

func (mm *monitorMetrics) clearDiskErrMetrics() {
	for k, v := range mm.badDisks {
		mm.diskError.DeleteLabelValues(v, k)
	}
}
func (mm *monitorMetrics) resetFollowerMetrics() {
	mm.masterNoCache.GaugeVec.Reset()
	mm.masterNoLeader.Set(0)
	mm.masterSnapshot.Set(0)
}

func (mm *monitorMetrics) resetAllLeaderMetrics() {
	mm.clearVolMetrics()
	mm.clearDiskErrMetrics()

	mm.dataNodesCount.Set(0)
	mm.metaNodesCount.Set(0)
	mm.volCount.Set(0)
	mm.dataNodesTotal.Set(0)
	mm.dataNodesUsed.Set(0)
	mm.dataNodeIncreased.Set(0)
	mm.metaNodesTotal.Set(0)
	mm.metaNodesUsed.Set(0)
	mm.metaNodesIncreased.Set(0)
	//mm.diskError.Set(0)
	mm.dataNodesInactive.Set(0)
	mm.metaNodesInactive.Set(0)
	mm.clearInconsistentMps()
}
