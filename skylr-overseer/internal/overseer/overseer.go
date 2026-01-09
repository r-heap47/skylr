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

// Overseer - Shard coordinator
type Overseer struct {
	shards   []shard
	shardsMu *sync.RWMutex
}

// shard - Shard data used by Overseer
type shard struct {
	addr    string
	errChan <-chan error
}

// New creates new Overseer
func New() *Overseer {
	ovr := &Overseer{
		shards:   []shard{},
		shardsMu: &sync.RWMutex{},
	}

	// start shard failure check
	go ovr.checkForShardFailures()

	return ovr
}

// Register registers new Shard onto Overseer
func (ovr *Overseer) Register(ctx context.Context, addr string) error {
	if err := utils.CtxDone(ctx); err != nil {
		return err
	}

	// check if shard by provided address is reachable and alive
	shardConn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("couldn't create shard conn: %w", err)
	}

	shardClient := pbshard.NewShardClient(shardConn)

	_, err = shardClient.Ping(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("couldn't ping shard: %w", err)
	}

	shardErrChan := make(chan error)

	// if shard is reachable and alive - start heartbeat check
	obs := &observer{
		shardClient: shardClient,
		errChan:     shardErrChan,
		delay:       func(_ context.Context) time.Duration { return time.Second },
	}

	// start shard healtcheck
	go obs.observe()

	ovr.appendShardAndReshard(shard{
		addr:    addr,
		errChan: shardErrChan,
	})

	return nil
}

// checkForShardFailures checks that none of the shards have disconnected
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

// appendShardAndReshard adds a new shard on Overseer and reshards inside a single lock acquisition
func (ovr *Overseer) appendShardAndReshard(shard shard) {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	ovr.shards = append(ovr.shards, shard)
	log.Printf("[INFO] New shard added successfully! Current shards: %+v\n", ovr.shards)
	log.Printf("[WARN] APPEND RESHARDING WOULD BE INITIATED HERE\n")
}

// removeShardAndReshard removes an old shard from Overseer and reshards inside a single lock acquisition
func (ovr *Overseer) removeShardAndReshard(oldShard shard) {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	for i, shard := range ovr.shards {
		if shard.addr == oldShard.addr {
			ovr.shards = append(ovr.shards[:i], ovr.shards[i+1:]...)
			break
		}
	}

	log.Printf("[INFO] Shard removed successfully! Current shards: %+v\n", ovr.shards)
	log.Printf("[WARN] REMOVE RESHARDING WOULD BE INITIATED HERE\n")
}
