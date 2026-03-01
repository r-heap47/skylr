package autoscaler

import "fmt"

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

	perShard := agg.TotalItems / uint64(agg.ShardCount)
	if perShard >= r.Threshold {
		return true, fmt.Sprintf("items/shard %d >= threshold %d (total=%d shards=%d)",
			perShard, r.Threshold, agg.TotalItems, agg.ShardCount)
	}

	return false, ""
}
