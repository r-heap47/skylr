package overseer

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/cutlery47/skylr/skylr-overseer/internal/pkg/utils"
)

type Overseer struct {
	shards   []string // shards ipv4 addrs
	shardsMu *sync.RWMutex
}

type Config struct {
	Shards   []string
	ShardsMu *sync.RWMutex
}

func New(cfg Config) *Overseer {
	return &Overseer{
		shards:   cfg.Shards,
		shardsMu: cfg.ShardsMu,
	}
}

func (ovr *Overseer) Register(ctx context.Context, addr string) error {
	if err := utils.CtxDone(ctx); err != nil {
		return err
	}

	err := ovr.addShard(addr)
	if err != nil {
		return fmt.Errorf("addShard: %w", err)
	}

	err = ovr.Reshard(ctx)
	if err != nil {
		return fmt.Errorf("Reshard: %w", err)
	}

	return nil
}

func (ovr *Overseer) addShard(addr string) error {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	ovr.shards = append(ovr.shards, addr)
	log.Printf("[INFO] New shard added successfully! Current shards: %v\n", ovr.shards)

	return nil
}

func (ovr *Overseer) Reshard(ctx context.Context) error {
	if err := utils.CtxDone(ctx); err != nil {
		return err
	}

	log.Println("[INFO]: Reshard: resharding to be impl here")

	return nil
}
