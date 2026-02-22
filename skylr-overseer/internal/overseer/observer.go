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
	errChan     chan<- error

	delay          utils.Provider[time.Duration]
	metricsTimeout utils.Provider[time.Duration]
	errorThreshold utils.Provider[int]
}

// observe monitors a single shard until ctx is cancelled.
// After errorThreshold consecutive Metrics failures it signals errChan and stops.
func (obs *observer) observe(ctx context.Context) {
	// poll on every tick; threshold is resolved once per observe lifetime
	ticker := time.NewTicker(obs.delay(ctx))
	defer ticker.Stop()

	var (
		consecutiveErrors         int
		consecutiveErrorsTheshold = obs.errorThreshold(ctx)
	)

	for {
		// stop when the overseer shuts down or when the shard is deregistered
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// collect metrics with a bounded timeout so a hung shard never blocks the loop
		metrics, err := func() (*pbshard.MetricsResponse, error) {
			rpcCtx, cancel := context.WithTimeout(ctx, obs.metricsTimeout(ctx))
			defer cancel()

			return obs.shardClient.Metrics(rpcCtx, &emptypb.Empty{})
		}()

		if err != nil {
			consecutiveErrors++

			log.Printf("[ERROR] shard %s: metrics error (%d/%d): %s",
				obs.addr, consecutiveErrors, consecutiveErrorsTheshold, err)

			// only signal failure after threshold is reached to tolerate transient errors
			if consecutiveErrors >= consecutiveErrorsTheshold {
				obs.errChan <- err
				return
			}

			continue
		}

		// successful poll resets the error streak
		consecutiveErrors = 0

		log.Printf("[INFO] metrics from %s: CPU=%.2f%%, RSS=%d MB, Heap=%d MB, Gets=%d, Sets=%d, Deletes=%d, Uptime=%ds",
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
