package overseer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pbshard "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Overseer - Shard coordinator
type Overseer struct {
	shards   map[string]shard
	shardsMu *sync.RWMutex

	ovsCtx context.Context // root context; lives for the entire overseer lifetime

	observerErrorThreshold     utils.Provider[int]
	observerMetricsTimeout     utils.Provider[time.Duration]
	checkForShardFailuresDelay utils.Provider[time.Duration]
	observerDelay              utils.Provider[time.Duration]
}

// shard - Shard data used by Overseer
type shard struct {
	addr    string
	conn    *grpc.ClientConn
	cancel  context.CancelFunc // stops the observer goroutine
	errChan <-chan error
}

// Config - Overseer config
type Config struct {
	CheckForShardFailuresDelay utils.Provider[time.Duration]
	ObserverDelay              utils.Provider[time.Duration]
	ObserverMetricsTimeout     utils.Provider[time.Duration]
	// ObserverErrorThreshold is the number of consecutive Metrics errors
	// before a shard is considered failed and removed.
	ObserverErrorThreshold utils.Provider[int]
}

// New creates new Overseer
func New(ovsCtx context.Context, cfg Config) *Overseer {
	ovr := &Overseer{
		shards:                     make(map[string]shard),
		shardsMu:                   &sync.RWMutex{},
		ovsCtx:                     ovsCtx,
		observerErrorThreshold:     cfg.ObserverErrorThreshold,
		observerMetricsTimeout:     cfg.ObserverMetricsTimeout,
		checkForShardFailuresDelay: cfg.CheckForShardFailuresDelay,
		observerDelay:              cfg.ObserverDelay,
	}

	go ovr.checkForShardFailures(ovsCtx)

	return ovr
}

// Register registers new Shard onto Overseer
func (ovr *Overseer) Register(ctx context.Context, addr string) error {
	if err := utils.CtxDone(ctx); err != nil {
		return err
	}

	// deduplicate: lookup by addr
	ovr.shardsMu.RLock()
	_, exists := ovr.shards[addr]
	ovr.shardsMu.RUnlock()
	if exists {
		return fmt.Errorf("shard %q is already registered", addr)
	}

	shardConn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("couldn't create shard conn: %w", err)
	}

	shardClient := pbshard.NewShardClient(shardConn)
	shardErrChan := make(chan error, 1)

	// obsCtx is derived from the overseer's root context (not the per-RPC ctx)
	// so the observer lives for the full overseer lifetime, not just one RPC call.
	obsCtx, obsCancel := context.WithCancel(ovr.ovsCtx)

	obs := &observer{
		addr:           addr,
		shardClient:    shardClient,
		errChan:        shardErrChan,
		delay:          ovr.observerDelay,
		metricsTimeout: ovr.observerMetricsTimeout,
		errorThreshold: ovr.observerErrorThreshold,
	}

	// add shard to the list before starting the observer so that
	// checkForShardFailures can always find it when errChan fires
	ovr.addShardAndReshardLocked(shard{
		addr:    addr,
		conn:    shardConn,
		cancel:  obsCancel,
		errChan: shardErrChan,
	})

	go obs.observe(obsCtx)

	return nil
}

// ShardCount returns the number of currently registered shards.
func (ovr *Overseer) ShardCount() int {
	ovr.shardsMu.RLock()
	defer ovr.shardsMu.RUnlock()
	return len(ovr.shards)
}

// checkForShardFailures checks that none of the shards have disconnected.
// It stops when ctx is cancelled.
func (ovr *Overseer) checkForShardFailures(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		func() {
			ovr.shardsMu.Lock()
			defer ovr.shardsMu.Unlock()

			for addr, s := range ovr.shards {
				select {
				case err := <-s.errChan:
					log.Printf("[ERROR] shard %s reported failure: %s", addr, err)
					ovr.removeShardAndReshard(s)
				default:
				}
			}
		}()

		time.Sleep(ovr.checkForShardFailuresDelay(ctx))
	}
}

// addShardAndReshardLocked acquires the write lock and adds the shard.
func (ovr *Overseer) addShardAndReshardLocked(s shard) {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	ovr.addShardAndReshard(s)
}

// addShardAndReshard adds a new shard. Must be called with shardsMu held.
func (ovr *Overseer) addShardAndReshard(s shard) {
	ovr.shards[s.addr] = s
	log.Printf("[INFO] shard %s added. total shards: %d\n", s.addr, len(ovr.shards))
	log.Printf("[WARN] APPEND RESHARDING WOULD BE INITIATED HERE\n")
}

// removeShardAndReshardLocked acquires the write lock and removes the shard.
// nolint
func (ovr *Overseer) removeShardAndReshardLocked(oldShard shard) {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	ovr.removeShardAndReshard(oldShard)
}

// removeShardAndReshard removes a shard and releases its resources.
// Must be called with shardsMu held.
func (ovr *Overseer) removeShardAndReshard(oldShard shard) {
	delete(ovr.shards, oldShard.addr)

	// stop the observer goroutine and close the gRPC connection
	oldShard.cancel()
	if err := oldShard.conn.Close(); err != nil {
		log.Printf("[WARN] error closing conn to shard %s: %s", oldShard.addr, err)
	}

	log.Printf("[INFO] shard %s removed. total shards: %d\n", oldShard.addr, len(ovr.shards))
	log.Printf("[WARN] REMOVE RESHARDING WOULD BE INITIATED HERE\n")
}
