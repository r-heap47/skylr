package overseer

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	pbshard "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// fakeShard is a minimal gRPC server that records Set/Delete calls and serves a fixed scan.
type fakeShard struct {
	pbshard.UnimplementedShardServer
	scanEntries []*pbshard.ScanResponse

	setCalls    []*pbshard.SetRequest
	deleteCalls []*pbshard.DeleteRequest

	// deleteErr, if set, causes Delete to return this error instead of success.
	deleteErr error
}

func (f *fakeShard) Scan(_ *emptypb.Empty, stream pbshard.Shard_ScanServer) error {
	for _, e := range f.scanEntries {
		if err := stream.Send(e); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeShard) Set(_ context.Context, req *pbshard.SetRequest) (*emptypb.Empty, error) {
	f.setCalls = append(f.setCalls, req)
	return &emptypb.Empty{}, nil
}

func (f *fakeShard) Delete(_ context.Context, req *pbshard.DeleteRequest) (*pbshard.DeleteResponse, error) {
	f.deleteCalls = append(f.deleteCalls, req)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &pbshard.DeleteResponse{Deleted: true}, nil
}

func (f *fakeShard) Get(_ context.Context, _ *pbshard.GetRequest) (*pbshard.GetResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in migration tests")
}

func (f *fakeShard) Metrics(_ context.Context, _ *emptypb.Empty) (*pbshard.MetricsResponse, error) {
	return &pbshard.MetricsResponse{}, nil
}

// startFakeShard starts a real gRPC server backed by fakeShard on a random port.
// Returns the server, its address, and a cleanup function.
func startFakeShard(t *testing.T, fs *fakeShard) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	pbshard.RegisterShardServer(srv, fs)

	go func() {
		_ = srv.Serve(lis)
	}()

	return lis.Addr().String(), func() {
		srv.GracefulStop()
	}
}

// newShardConn creates a gRPC client connection to addr.
func newShardConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn
}

// TestMigrateKeys_MovesKeysToNewShard verifies that migrateKeys scans source
// shards and moves only keys that belong to the new shard per the current ring.
func TestMigrateKeys_MovesKeysToNewShard(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Two entries on the source shard, TTL 30s each.
	ttl := durationpb.New(30 * time.Second)
	sourceEntries := []*pbshard.ScanResponse{
		{Entry: &pbshard.Entry{Key: "key-A", Value: &pbshard.Entry_ValueStr{ValueStr: "val-A"}}, RemainingTtl: ttl},
		{Entry: &pbshard.Entry{Key: "key-B", Value: &pbshard.Entry_ValueStr{ValueStr: "val-B"}}, RemainingTtl: ttl},
	}

	srcFake := &fakeShard{scanEntries: sourceEntries}
	dstFake := &fakeShard{}

	srcAddr, srcStop := startFakeShard(t, srcFake)
	defer srcStop()
	dstAddr, dstStop := startFakeShard(t, dstFake)
	defer dstStop()

	srcConn := newShardConn(t, srcAddr)
	dstConn := newShardConn(t, dstAddr)
	defer srcConn.Close()
	defer dstConn.Close()

	ovr := New(ctx, testConfig())

	// Manually add source shard (no observer, no migration)
	_, srcCancel := context.WithCancel(ctx)
	ovr.shardsMu.Lock()
	ovr.shards[srcAddr] = shard{
		addr:    srcAddr,
		conn:    srcConn,
		client:  pbshard.NewShardClient(srcConn),
		cancel:  srcCancel,
		errChan: make(chan error, 1),
	}
	ovr.ring.AddNode(srcAddr)
	ovr.shardsMu.Unlock()

	// Capture the old ring before adding the destination shard
	oldRing := ovr.ring.Snapshot()

	// Add destination shard to ring
	_, dstCancel := context.WithCancel(ctx)
	ovr.shardsMu.Lock()
	ovr.shards[dstAddr] = shard{
		addr:    dstAddr,
		conn:    dstConn,
		client:  pbshard.NewShardClient(dstConn),
		cancel:  dstCancel,
		errChan: make(chan error, 1),
	}
	ovr.ring.AddNode(dstAddr)
	ovr.shardsMu.Unlock()

	// Run migration synchronously by calling migrateKeys directly
	ovr.migrateKeys(ctx, oldRing, dstAddr)

	// For each key that now maps to dstAddr, it should have been Set on dst
	// and Deleted from src.
	for _, e := range sourceEntries {
		key := e.Entry.Key
		owner, err := ovr.ring.GetNode(key)
		require.NoError(t, err)

		if owner == dstAddr {
			// key should appear in dst.setCalls
			found := false
			for _, sc := range dstFake.setCalls {
				if sc.Input.Entry.Key == key {
					found = true
					break
				}
			}
			assert.True(t, found, "key %q should have been Set on destination shard", key)

			// key should appear in src.deleteCalls
			deleted := false
			for _, dc := range srcFake.deleteCalls {
				if dc.Key == key {
					deleted = true
					break
				}
			}
			assert.True(t, deleted, "key %q should have been Deleted from source shard", key)
		} else {
			// key stays on source, should NOT appear in dst
			for _, sc := range dstFake.setCalls {
				assert.NotEqual(t, key, sc.Input.Entry.Key, "key %q should NOT have been moved to destination", key)
			}
		}
	}
}

// TestMigrateKeys_NoSourceShards verifies that migration is a no-op when
// there are no source shards (first shard ever registered).
func TestMigrateKeys_NoSourceShards(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dstFake := &fakeShard{}
	dstAddr, dstStop := startFakeShard(t, dstFake)
	defer dstStop()

	dstConn := newShardConn(t, dstAddr)
	defer dstConn.Close()

	ovr := New(ctx, testConfig())

	emptyRing := ovr.ring.Snapshot()
	ovr.ring.AddNode(dstAddr)

	_, dstCancel := context.WithCancel(ctx)
	ovr.shardsMu.Lock()
	ovr.shards[dstAddr] = shard{
		addr:    dstAddr,
		conn:    dstConn,
		client:  pbshard.NewShardClient(dstConn),
		cancel:  dstCancel,
		errChan: make(chan error, 1),
	}
	ovr.shardsMu.Unlock()

	ovr.migrateKeys(ctx, emptyRing, dstAddr)

	assert.Empty(t, dstFake.setCalls, "no keys should be Set when there are no source shards")
}

// TestMigrateFromShard_DeleteFailure_NotCountedAsMoved verifies that when Delete
// fails on the source shard, moveKey returns (false, err) and the key is not
// counted as successfully moved.
func TestMigrateFromShard_DeleteFailure_NotCountedAsMoved(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srcFake := &fakeShard{deleteErr: status.Error(codes.Internal, "delete failed")}
	dstFake := &fakeShard{}

	srcAddr, srcStop := startFakeShard(t, srcFake)
	defer srcStop()
	dstAddr, dstStop := startFakeShard(t, dstFake)
	defer dstStop()

	srcConn := newShardConn(t, srcAddr)
	dstConn := newShardConn(t, dstAddr)
	defer srcConn.Close()
	defer dstConn.Close()

	ovr := New(ctx, testConfig())
	ovr.ring.AddNode(srcAddr)
	oldRing := ovr.ring.Snapshot()
	ovr.ring.AddNode(dstAddr)

	// Build source entries from keys that map to dst; ensures at least one migration candidate.
	ttl := durationpb.New(30 * time.Second)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if ovr.keyMovedToNewShard(key, oldRing, dstAddr) {
			srcFake.scanEntries = append(srcFake.scanEntries, &pbshard.ScanResponse{
				Entry:        &pbshard.Entry{Key: key, Value: &pbshard.Entry_ValueStr{ValueStr: "v"}},
				RemainingTtl: ttl,
			})
		}
	}
	require.NotEmpty(t, srcFake.scanEntries, "need at least one key that maps to dst with this ring topology")

	_, srcCancel := context.WithCancel(ctx)
	_, dstCancel := context.WithCancel(ctx)
	src := shard{addr: srcAddr, conn: srcConn, client: pbshard.NewShardClient(srcConn), cancel: srcCancel, errChan: make(chan error, 1)}
	dst := shard{addr: dstAddr, conn: dstConn, client: pbshard.NewShardClient(dstConn), cancel: dstCancel, errChan: make(chan error, 1)}

	moved, err := ovr.migrateFromShard(ctx, src, dst, oldRing, dstAddr)

	require.NoError(t, err)
	// moveKey returns (false, err) when Delete fails, so none are counted as moved.
	assert.Equal(t, 0, moved, "when Delete fails, moved must be 0")
	// Set was still attempted for migration candidates (we do Set before Delete)
	assert.NotEmpty(t, dstFake.setCalls, "Set should have been called for keys that map to dst")
	assert.Equal(t, len(dstFake.setCalls), len(srcFake.deleteCalls), "Delete attempted for each Set")
}

// TestMigrateFromShard_ScanEOF verifies that migrateFromShard handles a source
// shard with zero entries cleanly (EOF with no error).
func TestMigrateFromShard_ScanEOF(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srcFake := &fakeShard{} // empty
	dstFake := &fakeShard{}

	srcAddr, srcStop := startFakeShard(t, srcFake)
	defer srcStop()
	dstAddr, dstStop := startFakeShard(t, dstFake)
	defer dstStop()

	srcConn := newShardConn(t, srcAddr)
	dstConn := newShardConn(t, dstAddr)
	defer srcConn.Close()
	defer dstConn.Close()

	ovr := New(ctx, testConfig())
	ovr.ring.AddNode(srcAddr)
	oldRing := ovr.ring.Snapshot()
	ovr.ring.AddNode(dstAddr)

	_, srcCancel := context.WithCancel(ctx)
	_, dstCancel := context.WithCancel(ctx)
	src := shard{addr: srcAddr, conn: srcConn, client: pbshard.NewShardClient(srcConn), cancel: srcCancel, errChan: make(chan error, 1)}
	dst := shard{addr: dstAddr, conn: dstConn, client: pbshard.NewShardClient(dstConn), cancel: dstCancel, errChan: make(chan error, 1)}

	moved, err := ovr.migrateFromShard(ctx, src, dst, oldRing, dstAddr)

	require.NoError(t, err)
	assert.Equal(t, 0, moved)
	assert.Empty(t, dstFake.setCalls)
}

// Verify that isStreamEOF correctly identifies io.EOF.
func TestIsStreamEOF(t *testing.T) {
	assert.True(t, isStreamEOF(io.EOF))
	assert.False(t, isStreamEOF(nil))
	assert.False(t, isStreamEOF(io.ErrUnexpectedEOF))
}
