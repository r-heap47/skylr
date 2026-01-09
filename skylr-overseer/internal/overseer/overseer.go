package overseer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pbshard "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/cutlery47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Overseer struct {
	shards   []shardMeta
	shardsMu *sync.RWMutex
}

type shardMeta struct {
	addr    string
	errChan <-chan error
}

func New() *Overseer {
	ovr := &Overseer{
		shards:   []shardMeta{},
		shardsMu: &sync.RWMutex{},
	}

	go ovr.checkForShardFailures()

	return ovr
}

func (ovr *Overseer) Register(ctx context.Context, addr string) error {
	if err := utils.CtxDone(ctx); err != nil {
		return err
	}

	// check if provided address is reachable
	shardConn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("couldn't create shard conn: %w", err)
	}

	shardClient := pbshard.NewShardClient(shardConn)

	// check if shard is pingable
	_, err = shardClient.Ping(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("couldn't ping shard: %w", err)
	}

	shardErrChan := make(chan error)

	obs := &Observer{
		shardClient: shardClient,
		shardChan:   shardErrChan,
		delay:       func(_ context.Context) time.Duration { return time.Second },
	}

	// start shard healtcheck
	go obs.Observe()

	ovr.appendShardAndReshard(shardMeta{
		addr:    addr,
		errChan: shardErrChan,
	})

	return nil
}

func (ovr *Overseer) checkForShardFailures() {
	for {
		for _, shard := range ovr.shards {
			select {
			case err := <-shard.errChan:
				ovr.removeShardAndReshard(shard)
				log.Printf("[ERROR] checkForShardFailures received: %s", err)
			default:
				// do nothing
			}
		}

	}

}

func (ovr *Overseer) appendShardAndReshard(meta shardMeta) {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	ovr.shards = append(ovr.shards, meta)
	log.Printf("[INFO] New shard added successfully! Current shards: %+v\n", ovr.shards)
	log.Printf("[WARN] APPEND RESHARDING WOULD BE INITIATED HERE\n")
}

func (ovr *Overseer) removeShardAndReshard(meta shardMeta) {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	for i, shard := range ovr.shards {
		if shard.addr == meta.addr {
			ovr.shards = append(ovr.shards[:i], ovr.shards[i+1:]...)
			break
		}
	}

	log.Printf("[INFO] Shard removed successfully! Current shards: %+v\n", ovr.shards)
	log.Printf("[WARN] REMOVE RESHARDING WOULD BE INITIATED HERE\n")
}
