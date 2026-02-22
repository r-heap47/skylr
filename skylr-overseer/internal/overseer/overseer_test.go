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
