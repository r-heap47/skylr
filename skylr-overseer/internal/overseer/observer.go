package overseer

import (
	"context"
	"time"

	pbshard "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/cutlery47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/protobuf/types/known/emptypb"
)

// observer - shard monitorer
type observer struct {
	shardClient pbshard.ShardClient
	errChan     chan<- error // channel for sending ping errors

	delay utils.Provider[time.Duration]
}

// observe monitors a single shard
func (obs *observer) observe() {
	for {
		ctx := context.Background()

		_, err := obs.shardClient.Ping(ctx, &emptypb.Empty{})
		if err != nil {
			obs.errChan <- err
			return
		}

		time.Sleep(obs.delay(ctx))
	}
}
