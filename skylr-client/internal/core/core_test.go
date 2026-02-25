package core

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	pbovr "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-overseer"
	pbshard "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-shard"
)

// fakeOverseer implements Overseer server. Lookup returns a fixed shard address.
type fakeOverseer struct {
	pbovr.UnimplementedOverseerServer
	shardAddr string
}

func (f *fakeOverseer) Lookup(_ context.Context, _ *pbovr.LookupRequest) (*pbovr.LookupResponse, error) {
	return &pbovr.LookupResponse{ShardAddress: f.shardAddr}, nil
}

// fakeShard implements Shard server with in-memory storage.
type fakeShard struct {
	pbshard.UnimplementedShardServer
	mu   sync.RWMutex
	data map[string]*pbshard.Entry
}

func newFakeShard() *fakeShard {
	return &fakeShard{data: make(map[string]*pbshard.Entry)}
}

func (f *fakeShard) Get(_ context.Context, req *pbshard.GetRequest) (*pbshard.GetResponse, error) {
	f.mu.RLock()
	entry := f.data[req.Key]
	f.mu.RUnlock()
	if entry == nil {
		return nil, nil // NotFound would be proper; test uses nil for missing
	}
	return &pbshard.GetResponse{Entry: entry}, nil
}

func (f *fakeShard) Set(_ context.Context, req *pbshard.SetRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	f.data[req.Input.Entry.Key] = req.Input.Entry
	f.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (f *fakeShard) Delete(_ context.Context, req *pbshard.DeleteRequest) (*pbshard.DeleteResponse, error) {
	f.mu.Lock()
	_, existed := f.data[req.Key]
	delete(f.data, req.Key)
	f.mu.Unlock()
	return &pbshard.DeleteResponse{Deleted: existed}, nil
}

func (f *fakeShard) Metrics(context.Context, *emptypb.Empty) (*pbshard.MetricsResponse, error) {
	return &pbshard.MetricsResponse{}, nil
}

func (f *fakeShard) Scan(*emptypb.Empty, pbshard.Shard_ScanServer) error { return nil }

func setupTestServersTCP(t *testing.T) (ovrAddr, shardAddr string, cleanup func()) {
	t.Helper()

	ovrLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	shardLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ovrSrv := grpc.NewServer()
	pbovr.RegisterOverseerServer(ovrSrv, &fakeOverseer{shardAddr: shardLn.Addr().String()})
	go func() { _ = ovrSrv.Serve(ovrLn) }()

	shardSrv := grpc.NewServer()
	pbshard.RegisterShardServer(shardSrv, newFakeShard())
	go func() { _ = shardSrv.Serve(shardLn) }()

	cleanup = func() {
		ovrSrv.GracefulStop()
		shardSrv.GracefulStop()
	}

	return ovrLn.Addr().String(), shardLn.Addr().String(), cleanup
}

func TestClient_GetSetDelete(t *testing.T) {
	t.Parallel()

	ovrAddr, _, cleanup := setupTestServersTCP(t)
	defer cleanup()

	ctx := context.Background()
	c, err := New(ctx, ovrAddr, Config{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	// Set
	err = c.Set(ctx, "k1", "v1", time.Minute)
	require.NoError(t, err)

	// Get
	entry, err := c.Get(ctx, "k1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "k1", entry.Key)
	assert.Equal(t, "v1", entry.GetValueStr())

	// Delete
	deleted, err := c.Delete(ctx, "k1")
	require.NoError(t, err)
	assert.True(t, deleted)

	// Get after delete - shard returns nil entry for missing key in our fake
	entry2, err := c.Get(ctx, "k1")
	require.NoError(t, err)
	assert.Nil(t, entry2)
}

func TestClient_Set_ValueTypes(t *testing.T) {
	t.Parallel()

	ovrAddr, _, cleanup := setupTestServersTCP(t)
	defer cleanup()

	ctx := context.Background()
	c, err := New(ctx, ovrAddr, Config{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	tests := []struct {
		key   string
		value interface{}
	}{
		{"s", "str"},
		{"i32", int32(42)},
		{"i64", int64(123)},
		{"f32", float32(1.5)},
		{"f64", float64(2.5)},
	}
	for _, tt := range tests {
		err := c.Set(ctx, tt.key, tt.value, time.Minute)
		require.NoError(t, err)
	}
}

func TestClient_Set_UnsupportedValueType(t *testing.T) {
	t.Parallel()

	ovrAddr, _, cleanup := setupTestServersTCP(t)
	defer cleanup()

	ctx := context.Background()
	c, err := New(ctx, ovrAddr, Config{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	err = c.Set(ctx, "k", []byte("bad"), time.Minute)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported value type")
}
