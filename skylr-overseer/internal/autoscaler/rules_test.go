package autoscaler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestItemCountRule_Evaluate(t *testing.T) {
	t.Parallel()

	rule := ItemCountRule{Threshold: 10}

	tests := []struct {
		name        string
		agg         AggregatedMetrics
		wantTrigger bool
	}{
		{
			name:        "no shards — no trigger",
			agg:         AggregatedMetrics{ShardCount: 0, TotalItems: 100},
			wantTrigger: false,
		},
		{
			name:        "below threshold — no trigger",
			agg:         AggregatedMetrics{ShardCount: 2, TotalItems: 10}, // 5/shard
			wantTrigger: false,
		},
		{
			name:        "exactly at threshold — triggers",
			agg:         AggregatedMetrics{ShardCount: 2, TotalItems: 20}, // 10/shard
			wantTrigger: true,
		},
		{
			name:        "above threshold — triggers",
			agg:         AggregatedMetrics{ShardCount: 1, TotalItems: 15}, // 15/shard
			wantTrigger: true,
		},
		{
			name:        "scale-up effect: more shards drop avg below threshold",
			agg:         AggregatedMetrics{ShardCount: 3, TotalItems: 20}, // 6/shard
			wantTrigger: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			triggered, reason := rule.Evaluate(tc.agg)
			assert.Equal(t, tc.wantTrigger, triggered)
			if tc.wantTrigger {
				assert.NotEmpty(t, reason)
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}

func TestCPURule_Evaluate(t *testing.T) {
	t.Parallel()

	rule := CPURule{Threshold: 80.0}

	tests := []struct {
		name        string
		agg         AggregatedMetrics
		wantTrigger bool
	}{
		{
			name:        "below threshold — no trigger",
			agg:         AggregatedMetrics{AvgCPU: 50.0},
			wantTrigger: false,
		},
		{
			name:        "exactly at threshold — triggers",
			agg:         AggregatedMetrics{AvgCPU: 80.0},
			wantTrigger: true,
		},
		{
			name:        "above threshold — triggers",
			agg:         AggregatedMetrics{AvgCPU: 95.5},
			wantTrigger: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			triggered, reason := rule.Evaluate(tc.agg)
			assert.Equal(t, tc.wantTrigger, triggered)
			if tc.wantTrigger {
				assert.NotEmpty(t, reason)
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}

func TestMemoryRule_Evaluate(t *testing.T) {
	t.Parallel()

	rule := MemoryRule{ThresholdBytes: 512 * 1024 * 1024} // 512 MiB

	tests := []struct {
		name        string
		agg         AggregatedMetrics
		wantTrigger bool
	}{
		{
			name:        "no shards — no trigger",
			agg:         AggregatedMetrics{ShardCount: 0, TotalRSSBytes: 1024 * 1024 * 1024},
			wantTrigger: false,
		},
		{
			name:        "below threshold — no trigger",
			agg:         AggregatedMetrics{ShardCount: 2, TotalRSSBytes: 600 * 1024 * 1024}, // 300 MiB/shard
			wantTrigger: false,
		},
		{
			name:        "exactly at threshold — triggers",
			agg:         AggregatedMetrics{ShardCount: 2, TotalRSSBytes: 1024 * 1024 * 1024}, // 512 MiB/shard
			wantTrigger: true,
		},
		{
			name:        "scale-up effect: more shards drop avg below threshold",
			agg:         AggregatedMetrics{ShardCount: 4, TotalRSSBytes: 1024 * 1024 * 1024}, // 256 MiB/shard
			wantTrigger: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			triggered, reason := rule.Evaluate(tc.agg)
			assert.Equal(t, tc.wantTrigger, triggered)
			if tc.wantTrigger {
				assert.NotEmpty(t, reason)
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}

func TestThroughputRule_Evaluate(t *testing.T) {
	t.Parallel()

	t.Run("first call — no delta yet, no trigger", func(t *testing.T) {
		t.Parallel()

		rule := &ThroughputRule{Threshold: 100.0}
		triggered, reason := rule.Evaluate(AggregatedMetrics{ShardCount: 1, TotalGets: 500})
		assert.False(t, triggered)
		assert.Empty(t, reason)
	})

	t.Run("second call with sufficient delta — triggers", func(t *testing.T) {
		t.Parallel()

		rule := &ThroughputRule{Threshold: 100.0}
		// Prime the state: 0 ops, 1 second ago.
		rule.prevOps = 0
		rule.prevTime = time.Now().Add(-time.Second)

		// 300 total ops in ~1s on 1 shard → 300 ops/sec/shard >= 100 threshold.
		triggered, reason := rule.Evaluate(AggregatedMetrics{ShardCount: 1, TotalGets: 300})
		assert.True(t, triggered)
		assert.NotEmpty(t, reason)
	})

	t.Run("second call below threshold — no trigger", func(t *testing.T) {
		t.Parallel()

		rule := &ThroughputRule{Threshold: 500.0}
		rule.prevOps = 0
		rule.prevTime = time.Now().Add(-time.Second)

		// 300 ops/sec/shard < 500 threshold.
		triggered, reason := rule.Evaluate(AggregatedMetrics{ShardCount: 1, TotalGets: 300})
		assert.False(t, triggered)
		assert.Empty(t, reason)
	})

	t.Run("state advances each tick — no double-counting", func(t *testing.T) {
		t.Parallel()

		rule := &ThroughputRule{Threshold: 100.0}
		rule.prevOps = 0
		rule.prevTime = time.Now().Add(-time.Second)

		// Tick 1: 300 ops → triggers.
		rule.Evaluate(AggregatedMetrics{ShardCount: 1, TotalGets: 300}) //nolint:errcheck

		// Tick 2: still 300 total ops (no new ops since tick 1) → delta=0, no trigger.
		triggered, _ := rule.Evaluate(AggregatedMetrics{ShardCount: 1, TotalGets: 300})
		assert.False(t, triggered)
	})

	t.Run("no shards — no trigger even with high ops", func(t *testing.T) {
		t.Parallel()

		rule := &ThroughputRule{Threshold: 1.0}
		rule.prevOps = 0
		rule.prevTime = time.Now().Add(-time.Second)

		triggered, reason := rule.Evaluate(AggregatedMetrics{ShardCount: 0, TotalGets: 9999})
		assert.False(t, triggered)
		assert.Empty(t, reason)
	})
}
