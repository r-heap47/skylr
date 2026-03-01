package overseer

import "github.com/r-heap47/skylr/skylr-overseer/internal/autoscaler"

// CollectAggregatedMetrics reads the cached metrics from every observer and
// returns an aggregated snapshot. Shards whose observer has not yet produced
// a metrics poll are skipped.
//
// Satisfies autoscaler.MetricsCollector.
func (ovr *Overseer) CollectAggregatedMetrics() autoscaler.AggregatedMetrics {
	ovr.shardsMu.RLock()
	defer ovr.shardsMu.RUnlock()

	var agg autoscaler.AggregatedMetrics
	var cpuSum float64

	for _, s := range ovr.shards {
		m := s.obs.LastMetrics()
		if m == nil {
			continue
		}

		agg.ShardCount++
		agg.TotalItems += m.ItemCount
		agg.TotalRSSBytes += m.MemoryRssBytes
		agg.TotalGets += m.TotalGets
		agg.TotalSets += m.TotalSets
		agg.TotalDeletes += m.TotalDeletes
		cpuSum += m.CpuUsage
	}

	if agg.ShardCount > 0 {
		agg.AvgCPU = cpuSum / float64(agg.ShardCount)
	}

	return agg
}
