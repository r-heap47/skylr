package overseer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/r-heap47/skylr/skylr-overseer/internal/hashring"
	pbshard "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// defaultVirtualNodes is used when VirtualNodesPerShard is not configured.
const defaultVirtualNodes = 150

// Overseer - Shard coordinator
type Overseer struct {
	shards   map[string]shard
	shardsMu *sync.RWMutex

	ring   *hashring.HashRing
	ovsCtx context.Context // root context; lives for the entire overseer lifetime

	observerErrorThreshold     utils.Provider[int]
	observerMetricsTimeout     utils.Provider[time.Duration]
	checkForShardFailuresDelay utils.Provider[time.Duration]
	observerDelay              utils.Provider[time.Duration]
	logStorageOnMetrics        utils.Provider[bool]
}

// shard - Shard data used by Overseer
type shard struct {
	addr    string
	conn    *grpc.ClientConn
	client  pbshard.ShardClient
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
	// VirtualNodesPerShard is the number of virtual nodes placed on the consistent
	// hash ring per physical shard. Defaults to 150.
	VirtualNodesPerShard utils.Provider[int]
	// LogStorageOnMetrics, when true, logs all storage entries after each metrics
	// poll (for debug mode, e.g. with process provisioner).
	LogStorageOnMetrics utils.Provider[bool]
}

// New creates new Overseer
func New(ovsCtx context.Context, cfg Config) *Overseer {
	vnodeCount := defaultVirtualNodes
	if cfg.VirtualNodesPerShard != nil {
		if v := cfg.VirtualNodesPerShard(ovsCtx); v > 0 {
			vnodeCount = v
		}
	}

	ovr := &Overseer{
		shards:                     make(map[string]shard),
		shardsMu:                   &sync.RWMutex{},
		ring:                       hashring.New(vnodeCount),
		ovsCtx:                     ovsCtx,
		observerErrorThreshold:     cfg.ObserverErrorThreshold,
		observerMetricsTimeout:     cfg.ObserverMetricsTimeout,
		checkForShardFailuresDelay: cfg.CheckForShardFailuresDelay,
		observerDelay:              cfg.ObserverDelay,
		logStorageOnMetrics:        cfg.LogStorageOnMetrics,
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
		addr:                addr,
		shardClient:         shardClient,
		errChan:             shardErrChan,
		delay:               ovr.observerDelay,
		metricsTimeout:      ovr.observerMetricsTimeout,
		errorThreshold:      ovr.observerErrorThreshold,
		logStorageOnMetrics: ovr.logStorageOnMetrics,
	}

	// add shard to the list before starting the observer so that
	// checkForShardFailures can always find it when errChan fires
	ovr.addShardAndReshardLocked(shard{
		addr:    addr,
		conn:    shardConn,
		client:  shardClient,
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

// HasShard returns true if addr is registered on the overseer.
func (ovr *Overseer) HasShard(addr string) bool {
	ovr.shardsMu.RLock()
	defer ovr.shardsMu.RUnlock()

	_, ok := ovr.shards[addr]
	return ok
}

// Unregister removes the shard at addr from the ring and releases its resources.
func (ovr *Overseer) Unregister(addr string) error {
	ovr.shardsMu.Lock()
	defer ovr.shardsMu.Unlock()

	s, ok := ovr.shards[addr]
	if !ok {
		return fmt.Errorf("shard %q not found", addr)
	}
	ovr.removeShard(s)
	return nil
}

// Lookup returns the address of the shard responsible for the given key
// according to the consistent hash ring.
func (ovr *Overseer) Lookup(key string) (string, error) {
	addr, err := ovr.ring.GetNode(key)
	if err != nil {
		return "", fmt.Errorf("ring.GetNode: %w", err)
	}

	return addr, nil
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
					ovr.removeShard(s)
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

// addShardAndReshard adds a new shard and updates the hash ring.
// Must be called with shardsMu held.
func (ovr *Overseer) addShardAndReshard(s shard) {
	oldRing := ovr.ring.Snapshot()
	ovr.ring.AddNode(s.addr)
	ovr.shards[s.addr] = s

	log.Printf("[INFO] shard %s added. total shards: %d\n", s.addr, len(ovr.shards))

	go ovr.migrateKeys(ovr.ovsCtx, oldRing, s.addr)
}

// removeShard removes a shard, releases its resources, and updates the hash ring.
// Must be called with shardsMu held.
// When a shard fails its in-memory data is lost; the ring update ensures future
// lookups are routed to the remaining shards.
func (ovr *Overseer) removeShard(oldShard shard) {
	ovr.ring.RemoveNode(oldShard.addr)
	delete(ovr.shards, oldShard.addr)

	// stop the observer goroutine and close the gRPC connection
	oldShard.cancel()
	if err := oldShard.conn.Close(); err != nil {
		log.Printf("[WARN] error closing conn to shard %s: %s", oldShard.addr, err)
	}

	log.Printf("[INFO] shard %s removed. total shards: %d\n", oldShard.addr, len(ovr.shards))
}
