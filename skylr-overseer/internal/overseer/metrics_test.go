package overseer

import (
	"context"
	"testing"

	"github.com/r-heap47/skylr/skylr-overseer/internal/autoscaler"
	pbshard "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/stretchr/testify/assert"
)

// shardWithMetrics registers a fake shard whose observer already has a cached
// metrics snapshot. Does not start a real observer goroutine.
func shardWithMetrics(ovr *Overseer, addr string, m *pbshard.MetricsResponse) {
	conn, _ := dialFakeConn()
	_, cancel := context.WithCancel(ovr.ovsCtx)
	obs := &observer{addr: addr}
	if m != nil {
		obs.lastMetrics.Store(m)
	}
	ovr.shardsMu.Lock()
	ovr.shards[addr] = shard{addr: addr, conn: conn, cancel: cancel, obs: obs}
	ovr.shardsMu.Unlock()
}

// TestCollectAggregatedMetrics_Empty verifies that an overseer with no shards
// returns a zero-value AggregatedMetrics.
func TestCollectAggregatedMetrics_Empty(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())
	agg := ovr.CollectAggregatedMetrics()

	assert.Equal(t, autoscaler.AggregatedMetrics{}, agg)
}

// TestCollectAggregatedMetrics_SkipsNilMetrics verifies that shards whose
// observer has never polled (nil lastMetrics) are excluded from the aggregate.
func TestCollectAggregatedMetrics_SkipsNilMetrics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())
	shardWithMetrics(ovr, "localhost:19100", nil)

	agg := ovr.CollectAggregatedMetrics()
	assert.Equal(t, 0, agg.ShardCount, "shard with nil metrics should be excluded")
}

// TestCollectAggregatedMetrics_Aggregates verifies that metrics are correctly
// summed across shards that have cached snapshots.
func TestCollectAggregatedMetrics_Aggregates(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())

	shardWithMetrics(ovr, "localhost:19101", &pbshard.MetricsResponse{
		ItemCount:      10,
		MemoryRssBytes: 100,
		TotalGets:      5,
		TotalSets:      3,
		TotalDeletes:   1,
		CpuUsage:       20.0,
	})
	shardWithMetrics(ovr, "localhost:19102", &pbshard.MetricsResponse{
		ItemCount:      30,
		MemoryRssBytes: 200,
		TotalGets:      10,
		TotalSets:      7,
		TotalDeletes:   2,
		CpuUsage:       40.0,
	})
	// third shard with no metrics yet — should be skipped
	shardWithMetrics(ovr, "localhost:19103", nil)

	agg := ovr.CollectAggregatedMetrics()

	assert.Equal(t, 2, agg.ShardCount)
	assert.Equal(t, uint64(40), agg.TotalItems)
	assert.Equal(t, uint64(300), agg.TotalRSSBytes)
	assert.Equal(t, uint64(15), agg.TotalGets)
	assert.Equal(t, uint64(10), agg.TotalSets)
	assert.Equal(t, uint64(3), agg.TotalDeletes)
	assert.InDelta(t, 30.0, agg.AvgCPU, 0.01)
}
