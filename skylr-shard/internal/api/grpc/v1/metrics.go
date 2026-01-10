package v1

import (
	"fmt"
	"time"

	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Metrics .
func (i *Implementation) Metrics(_ *emptypb.Empty, stream grpc.ServerStreamingServer[pbshard.MetricsResponse]) error {
	ctx := stream.Context()

	for {
		var res pbshard.MetricsResponse

		numElements, err := i.collector.NumElements(ctx)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("collector.NumElements: %s", err))
		}

		cpuUsage, err := i.collector.UsageCPU(ctx)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("collector.UsageCPU: %s", err))
		}

		res.NumElements = int64(numElements)
		res.CpuUsage = cpuUsage

		err = stream.Send(&res)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("steam.Send: %s", err))
		}

		time.Sleep(i.metricsDelay(ctx))
	}
}
