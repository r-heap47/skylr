package overseer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// dialFakeConn creates a grpc.ClientConn to a non-existent address.
// grpc.NewClient is lazy so this succeeds immediately.
func dialFakeConn() (*grpc.ClientConn, error) {
	return grpc.NewClient(
		"localhost:1", // no server; connection is lazy
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// testConfig returns an Overseer config with near-zero delays, suitable for tests.
func testConfig() Config {
	return Config{
		CheckForShardFailuresDelay: utils.Const(time.Millisecond),
		ObserverDelay:              utils.Const(time.Millisecond),
		ObserverMetricsTimeout:     utils.Const(100 * time.Millisecond),
		ObserverErrorThreshold:     utils.Const(3),
		VirtualNodesPerShard:       utils.Const(10),
	}
}

// TestRegister_HappyPath verifies that a valid address is successfully registered
// and appears in the Overseer's shard map.
func TestRegister_HappyPath(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())

	err := ovr.Register(ctx, "localhost:19000")
	require.NoError(t, err)
	assert.Equal(t, 1, ovr.ShardCount())
}

// TestRegister_Duplicate verifies that registering the same address twice
// returns an error and leaves exactly one shard in the map.
func TestRegister_Duplicate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())

	require.NoError(t, ovr.Register(ctx, "localhost:19001"))

	err := ovr.Register(ctx, "localhost:19001")
	require.Error(t, err)
	assert.Equal(t, 1, ovr.ShardCount())
}

// TestRegister_CancelledCtx verifies that Register with an already-cancelled
// context returns immediately with an error.
func TestRegister_CancelledCtx(t *testing.T) {
	t.Parallel()

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(rootCtx, testConfig())

	cancelledCtx, cancelReq := context.WithCancel(context.Background())
	cancelReq()

	err := ovr.Register(cancelledCtx, "localhost:19002")
	require.Error(t, err)
	assert.Equal(t, 0, ovr.ShardCount())
}

// TestLookup_EmptyRing verifies that Lookup returns an error when no shards are registered.
func TestLookup_EmptyRing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())

	_, err := ovr.Lookup("some-key")
	require.Error(t, err)
}

// TestLookup_ReturnsRegisteredShard verifies that Lookup returns the address of
// the registered shard after registration.
func TestLookup_ReturnsRegisteredShard(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())
	require.NoError(t, ovr.Register(ctx, "localhost:19010"))

	addr, err := ovr.Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, "localhost:19010", addr)
}

// TestLookup_Deterministic verifies that the same key always maps to the same shard.
func TestLookup_Deterministic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())
	require.NoError(t, ovr.Register(ctx, "localhost:19011"))
	require.NoError(t, ovr.Register(ctx, "localhost:19012"))

	const key = "deterministic-key"
	first, err := ovr.Lookup(key)
	require.NoError(t, err)

	for range 50 {
		addr, err := ovr.Lookup(key)
		require.NoError(t, err)
		assert.Equal(t, first, addr)
	}
}

// TestLookup_AfterShardRemoval verifies that after a shard is removed from the ring,
// Lookup routes exclusively to the remaining shard.
func TestLookup_AfterShardRemoval(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())

	conn1, err := dialFakeConn()
	require.NoError(t, err)
	conn2, err := dialFakeConn()
	require.NoError(t, err)

	_, obsCancel1 := context.WithCancel(ctx)
	_, obsCancel2 := context.WithCancel(ctx)

	errChan1 := make(chan error, 1)
	errChan2 := make(chan error, 1)

	ovr.addShardAndReshardLocked(shard{addr: "localhost:19020", conn: conn1, cancel: obsCancel1, errChan: errChan1})
	ovr.addShardAndReshardLocked(shard{addr: "localhost:19021", conn: conn2, cancel: obsCancel2, errChan: errChan2})

	require.Equal(t, 2, ovr.ShardCount())

	// remove shard19020 by directly calling the internal method
	ovr.shardsMu.Lock()
	s := ovr.shards["localhost:19020"]
	ovr.removeShardAndReshard(s)
	ovr.shardsMu.Unlock()

	require.Equal(t, 1, ovr.ShardCount())

	// every key must now map to the only remaining shard
	for _, key := range []string{"k1", "k2", "k3", "some-other-key"} {
		addr, err := ovr.Lookup(key)
		require.NoError(t, err)
		assert.Equal(t, "localhost:19021", addr, "key %q should map to the remaining shard", key)
	}
}

// TestCheckForShardFailures_RemovesFailedShard verifies that when errChan
// receives an error, checkForShardFailures removes the shard from the map.
func TestCheckForShardFailures_RemovesFailedShard(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ovr := New(ctx, testConfig())

	// add a shard manually bypassing grpc.NewClient so we control errChan directly
	errChan := make(chan error, 1)
	_, obsCancel := context.WithCancel(ctx)
	ovr.addShardAndReshardLocked(shard{
		addr:    "localhost:19003",
		conn:    nil,
		cancel:  obsCancel,
		errChan: errChan,
	})

	require.Equal(t, 1, ovr.ShardCount())

	// removeShardAndReshard calls conn.Close(), so nil would panic.
	// Replace the shard entry with one that has a real (lazy, unused) conn.
	conn, err := dialFakeConn()
	require.NoError(t, err)

	ovr.shardsMu.Lock()
	ovr.shards["localhost:19003"] = shard{
		addr:    "localhost:19003",
		conn:    conn,
		cancel:  obsCancel,
		errChan: errChan,
	}
	ovr.shardsMu.Unlock()

	// trigger failure
	errChan <- errors.New("shard went away")

	// wait for checkForShardFailures to pick it up
	assert.Eventually(t,
		func() bool { return ovr.ShardCount() == 0 },
		time.Second,
		5*time.Millisecond,
		"shard should be removed after errChan signal",
	)
}
