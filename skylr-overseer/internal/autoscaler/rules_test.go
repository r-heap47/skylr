package autoscaler

import (
	"testing"

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
