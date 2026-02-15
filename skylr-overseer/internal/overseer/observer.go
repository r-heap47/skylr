package overseer

import (
	"context"
	"log"
	"time"

	pbshard "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/protobuf/types/known/emptypb"
)

// observer - shard monitorer
type observer struct {
	addr        string
	shardClient pbshard.ShardClient
	errChan     chan<- error // channel for sending ping errors

	delay utils.Provider[time.Duration]
}

// observe monitors a single shard
func (obs *observer) observe() {
	ticker := time.NewTicker(1 * time.Second) // polling interval
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			metrics, err := obs.shardClient.Metrics(ctx, &emptypb.Empty{})
			cancel()

			if err != nil {
				// Only log the error, don't send to errChan
				// This prevents losing the shard on transient network issues
				log.Printf("[ERROR] Failed to collect metrics from %s: %s", obs.addr, err)
				continue // continue polling on next tick
			}

			// Stub: just log the metrics
			log.Printf("[INFO] Metrics from %s: CPU=%.2f%%, RSS=%d MB, Heap=%d MB, Gets=%d, Sets=%d, Deletes=%d, Uptime=%ds",
				obs.addr,
				metrics.CpuUsage,
				metrics.MemoryRssBytes/(1024*1024),
				metrics.MemoryHeapAllocBytes/(1024*1024),
				metrics.TotalGets,
				metrics.TotalSets,
				metrics.TotalDeletes,
				metrics.UptimeSeconds)
		}
	}
}
