package overseer

import (
	"context"
	"fmt"
	"log"
	"time"

	pbshard "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/cutlery47/skylr/skylr-overseer/internal/pkg/utils"
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
	ctx := context.Background()

	stream, err := obs.shardClient.Metrics(ctx, &emptypb.Empty{})
	if err != nil {
		obs.errChan <- fmt.Errorf("shardClient.Metrics: %w", err)
		return
	}

	for {
		metrics, err := stream.Recv()
		if err != nil {
			obs.errChan <- fmt.Errorf("stream.Recv: %w", err)
			return
		}

		log.Printf("[INFO] Received metrics from %s: %+v\n", obs.addr, metrics)
		log.Println("[WARN] Metric handling logic should be implemented here")
	}
}
