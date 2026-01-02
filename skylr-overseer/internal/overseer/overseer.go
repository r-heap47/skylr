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
	shardChans []<-chan error

	shards   []string // shards ipv4 addrs
	shardsMu *sync.RWMutex
}

type Config struct {
	Shards   []string
	ShardsMu *sync.RWMutex
}

func New(cfg Config) *Overseer {
	ovr := &Overseer{
		shardChans: []<-chan error{},
		shards:     cfg.Shards,
		shardsMu:   cfg.ShardsMu,
	}

	go ovr.slack()

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

	err = ovr.addShard(addr)
	if err != nil {
		return fmt.Errorf("addShard: %w", err)
	}

	err = ovr.Reshard(ctx)
	if err != nil {
		return fmt.Errorf("Reshard: %w", err)
	}

	errChan := make(chan error)
	ovr.shardChans = append(ovr.shardChans, errChan)

	obs := &Observer{
		shardClient: shardClient,
		errChan:     errChan,
		delay:       func(_ context.Context) time.Duration { return time.Second },
	}

	go obs.Observe()

	return nil
}

func (ovr *Overseer) slack() {
	for {
		
	}
}

func (ovr *Overseer) Reshard(ctx context.Context) error {
	if err := utils.CtxDone(ctx); err != nil {
		return err
	}

	log.Println("[INFO]: Reshard: resharding to be impl here")

	return nil
}

func (ovr *Overseer) addShard(addr string) error {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	ovr.shards = append(ovr.shards, addr)
	log.Printf("[INFO] New shard added successfully! Current shards: %v\n", ovr.shards)

	return nil
}

func (ovr *Overseer) rmShard(addr string) error {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	for i, shard := range ovr.shards {
		if shard == addr {
			ovr.shards = append(ovr.shards[:i], ovr.shards[i+1:]...)
			return nil
		}
	}

	return nil
}
