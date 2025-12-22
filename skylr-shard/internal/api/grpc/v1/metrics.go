package v1

import (
	"fmt"
	"time"

	pbshard "github.com/cutlery47/skylr/skylr-shard/internal/pb/skylr-shard"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Metrics .
func (i *Implementation) Metrics(_ *emptypb.Empty, stream grpc.ServerStreamingServer[pbshard.MetricsResponse]) error {
	ctx := stream.Context()

	for {
		var res pbshard.MetricsResponse

		numElements, err := i.collector.NumElements(ctx)
		if err != nil {
			return fmt.Errorf("collector.NumElements: %w", err)
		}

		cpuUsage, err := i.collector.UsageCPU(ctx)
		if err != nil {
			return fmt.Errorf("collector.UsageCPU: %w", err)
		}

		res.NumElements = int64(numElements)
		res.CpuUsage = cpuUsage

		err = stream.Send(&res)
		if err != nil {
			return fmt.Errorf("steam.Send: %w", err)
		}

		time.Sleep(time.Second)
	}
}
