package overseer

import (
	"context"
	"time"

	pbshard "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/cutlery47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Observer struct {
	shardClient pbshard.ShardClient
	errChan     chan<- error

	delay utils.Provider[time.Duration]
}

func (obs *Observer) Observe() {
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
