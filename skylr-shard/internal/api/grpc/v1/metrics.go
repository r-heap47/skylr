package v1

import (
	"context"
	"fmt"

	"github.com/r-heap47/skylr/skylr-shard/internal/metrics"
	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Metrics returns current service metrics
func (i *Implementation) Metrics(ctx context.Context, _ *emptypb.Empty) (*pbshard.MetricsResponse, error) {
	var (
		cpuUsage  float64
		rss       uint64
		heapAlloc uint64
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		v, err := i.collector.UsageCPU(ctx)
		cpuUsage = v
		return err
	})
	g.Go(func() error {
		v, err := i.collector.MemoryRSS(ctx)
		rss = v
		return err
	})
	g.Go(func() error {
		v, err := i.collector.MemoryHeapAlloc(ctx)
		heapAlloc = v
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("metrics collection: %s", err))
	}

	return &pbshard.MetricsResponse{
		CpuUsage:             cpuUsage,
		MemoryRssBytes:       rss,
		MemoryHeapAllocBytes: heapAlloc,
		TotalGets:            metrics.TotalGets(),
		TotalSets:            metrics.TotalSets(),
		TotalDeletes:         metrics.TotalDeletes(),
		UptimeSeconds:        i.collector.Uptime(),
	}, nil
}
