package overseer

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
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

	delay               utils.Provider[time.Duration]
	metricsTimeout      utils.Provider[time.Duration]
	errorThreshold      utils.Provider[int]
	logStorageOnMetrics utils.Provider[bool]
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
		resp, err := func() (*pbshard.MetricsResponse, error) {
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

		obs.logMetrics(resp)
		if obs.logStorageOnMetrics(ctx) {
			obs.logStorage(ctx)
		}
	}
}

// logMetrics logs the shard's metrics response.
func (obs *observer) logMetrics(resp *pbshard.MetricsResponse) {
	rssMB := float64(resp.MemoryRssBytes) / (1024 * 1024)
	heapMB := float64(resp.MemoryHeapAllocBytes) / (1024 * 1024)
	log.Printf("[INFO] shard %s metrics: cpu=%.2f%% rss=%.2f MB heap=%.2f MB gets=%d sets=%d deletes=%d uptime=%ds",
		obs.addr, resp.CpuUsage, rssMB, heapMB,
		resp.TotalGets, resp.TotalSets, resp.TotalDeletes, resp.UptimeSeconds)
}

// logStorage scans the shard's storage and logs all entries in one message.
func (obs *observer) logStorage(ctx context.Context) {
	rpcCtx, cancel := context.WithTimeout(ctx, obs.metricsTimeout(ctx))
	defer cancel()

	stream, err := obs.shardClient.Scan(rpcCtx, &emptypb.Empty{})
	if err != nil {
		log.Printf("[DEBUG] shard %s: scan failed: %s", obs.addr, err)
		return
	}

	var parts []string
	for {
		if ctx.Err() != nil {
			return
		}

		resp, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Printf("[DEBUG] shard %s: scan recv error: %s", obs.addr, err)
			}
			break
		}

		if resp.Entry != nil {
			parts = append(parts, formatEntry(resp.Entry))
		}
	}

	log.Printf("[DEBUG] shard %s storage (%d entries): %s", obs.addr, len(parts), strings.Join(parts, " "))
}

func formatEntry(e *pbshard.Entry) string {
	var v string
	switch e.Value.(type) {
	case *pbshard.Entry_ValueStr:
		v = fmt.Sprintf("%q", e.GetValueStr())
	case *pbshard.Entry_ValueInt32:
		v = fmt.Sprintf("%d", e.GetValueInt32())
	case *pbshard.Entry_ValueInt64:
		v = fmt.Sprintf("%d", e.GetValueInt64())
	case *pbshard.Entry_ValueFloat:
		v = fmt.Sprintf("%g", e.GetValueFloat())
	case *pbshard.Entry_ValueDouble:
		v = fmt.Sprintf("%g", e.GetValueDouble())
	default:
		v = "?"
	}
	return fmt.Sprintf("%s=%s", e.Key, v)
}
