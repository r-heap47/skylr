package autoscaler

import (
	"fmt"
	"time"
)

// ScalingRule evaluates aggregated metrics and decides whether a scale-up is warranted.
type ScalingRule interface {
	Evaluate(agg AggregatedMetrics) (triggered bool, reason string)
}

// ItemCountRule triggers a scale-up when the average number of items per shard
// reaches or exceeds Threshold. Using per-shard average (TotalItems / ShardCount)
// provides a natural dynamic threshold: after a scale-up the denominator grows,
// causing the metric to drop below the threshold without any state reset.
type ItemCountRule struct {
	Threshold uint64
}

// Evaluate implements ScalingRule.
func (r ItemCountRule) Evaluate(agg AggregatedMetrics) (bool, string) {
	if agg.ShardCount == 0 {
		return false, ""
	}

	perShard := agg.TotalItems / uint64(agg.ShardCount) //nolint:gosec // ShardCount is always positive (checked above)
	if perShard >= r.Threshold {
		return true, fmt.Sprintf("items/shard %d >= threshold %d (total=%d shards=%d)",
			perShard, r.Threshold, agg.TotalItems, agg.ShardCount)
	}

	return false, ""
}

// CPURule triggers a scale-up when the average CPU usage across shards reaches or exceeds Threshold.
type CPURule struct {
	Threshold float64 // percent, e.g. 80.0
}

// Evaluate implements ScalingRule.
func (r CPURule) Evaluate(agg AggregatedMetrics) (bool, string) {
	if agg.AvgCPU >= r.Threshold {
		return true, fmt.Sprintf("avg cpu %.1f%% >= threshold %.1f%%", agg.AvgCPU, r.Threshold)
	}

	return false, ""
}

// MemoryRule triggers a scale-up when the average RSS memory per shard reaches or exceeds ThresholdBytes.
type MemoryRule struct {
	ThresholdBytes uint64
}

// Evaluate implements ScalingRule.
func (r MemoryRule) Evaluate(agg AggregatedMetrics) (bool, string) {
	if agg.ShardCount == 0 {
		return false, ""
	}

	perShard := agg.TotalRSSBytes / uint64(agg.ShardCount) //nolint:gosec // ShardCount is always positive (checked above)
	if perShard >= r.ThresholdBytes {
		return true, fmt.Sprintf("rss/shard %d bytes >= threshold %d (total=%d shards=%d)",
			perShard, r.ThresholdBytes, agg.TotalRSSBytes, agg.ShardCount)
	}

	return false, ""
}

// ThroughputRule triggers a scale-up when the average number of operations per second per shard
// reaches or exceeds Threshold. It is stateful: each call computes the delta from the previous
// snapshot, so it must be held as a pointer in the rules slice.
type ThroughputRule struct {
	Threshold float64 // ops/sec per shard
	prevOps   uint64
	prevTime  time.Time
}

// Evaluate implements ScalingRule.
func (r *ThroughputRule) Evaluate(agg AggregatedMetrics) (bool, string) {
	currOps := agg.TotalGets + agg.TotalSets + agg.TotalDeletes
	now := time.Now()

	if r.prevTime.IsZero() {
		// First call — no delta yet; snapshot state and wait for next tick.
		r.prevOps = currOps
		r.prevTime = now

		return false, ""
	}

	elapsed := now.Sub(r.prevTime).Seconds()
	opsPerSec := float64(currOps-r.prevOps) / elapsed

	r.prevOps = currOps
	r.prevTime = now

	if agg.ShardCount == 0 {
		return false, ""
	}

	perShard := opsPerSec / float64(agg.ShardCount)
	if perShard >= r.Threshold {
		return true, fmt.Sprintf("ops/sec/shard %.1f >= threshold %.1f (total_ops/s=%.1f shards=%d)",
			perShard, r.Threshold, opsPerSec, agg.ShardCount)
	}

	return false, ""
}
