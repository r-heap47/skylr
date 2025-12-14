package shard

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/testutils"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage/storages/noeviction"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	defaultNow      = time.Now()
	defaultTimeout  = 5 * time.Second
	defaultCooldown = 5 * time.Second
)

type shardTestSuite struct {
	now      *time.Time
	timeout  *time.Duration
	cooldown *time.Duration

	nowCalled      atomic.Int64
	timeoutCalled  atomic.Int64
	cooldownCalled atomic.Int64

	storageStr     storage.Storage[string]
	storageInt64   storage.Storage[int64]
	storageInt32   storage.Storage[int32]
	storageFloat64 storage.Storage[float64]
	storageFloat32 storage.Storage[float32]

	sh *Shard

	suite.Suite
}

func TestShardTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(shardTestSuite))
}

func (s *shardTestSuite) SetupTest() {
	startCh := make(chan struct{})

	s.now = lo.ToPtr(defaultNow)
	s.timeout = lo.ToPtr(defaultTimeout)
	s.cooldown = lo.ToPtr(defaultCooldown)

	s.nowCalled = atomic.Int64{}
	s.timeoutCalled = atomic.Int64{}
	s.cooldownCalled = atomic.Int64{}

	curTime := func(_ context.Context) time.Time {
		s.nowCalled.Add(1)
		return *s.now
	}

	cleanupTimeout := func(_ context.Context) time.Duration {
		s.timeoutCalled.Add(1)
		return *s.timeout
	}

	cleanupCooldown := func(_ context.Context) time.Duration {
		s.cooldownCalled.Add(1)
		return *s.cooldown
	}

	storageStr := noeviction.New[string](noeviction.Config{
		CurTime: curTime,
	})

	storageInt64 := noeviction.New[int64](noeviction.Config{
		CurTime: curTime,
	})

	storageInt32 := noeviction.New[int32](noeviction.Config{
		CurTime: curTime,
	})

	storageFloat64 := noeviction.New[float64](noeviction.Config{
		CurTime: curTime,
	})

	storageFloat32 := noeviction.New[float32](noeviction.Config{
		CurTime: curTime,
	})

	s.storageStr = storageStr
	s.storageInt32 = storageInt32
	s.storageInt64 = storageInt64
	s.storageFloat32 = storageFloat32
	s.storageFloat64 = storageFloat64

	shard := New(Config{
		StorageStr:     storageStr,
		StorageInt64:   storageInt64,
		StorageInt32:   storageInt32,
		StorageFloat64: storageFloat64,
		StorageFloat32: storageFloat32,

		CurTime:         curTime,
		CleanupTimeout:  cleanupTimeout,
		CleanupCooldown: cleanupCooldown,

		Start: startCh,
	})

	s.sh = shard
}

func (s *shardTestSuite) TestCleanup_InfiniteWaitForStart() {
	require.Never(
		s.T(),
		func() bool {
			return s.cooldownCalled.Load() > 0
		},
		2*time.Second,
		100*time.Millisecond,
	)
}

func (s *shardTestSuite) TestClean_Success() {
	var (
		ctx = context.Background()

		now = testutils.MustParseDate(s.T(), "2025-01-01")
		exp = now.Add(100 * time.Microsecond)
	)

	_, err := s.storageFloat32.Set(ctx, storage.Entry[float32]{
		K:   "key",
		V:   1,
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storageFloat64.Set(ctx, storage.Entry[float64]{
		K:   "key",
		V:   1,
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storageInt32.Set(ctx, storage.Entry[int32]{
		K:   "key",
		V:   1,
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storageInt64.Set(ctx, storage.Entry[int64]{
		K:   "key",
		V:   1,
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storageStr.Set(ctx, storage.Entry[string]{
		K:   "key",
		V:   "val",
		Exp: exp,
	})
	s.Require().NoError(err)

	s.now = lo.ToPtr(exp.Add(time.Microsecond))

	err = s.sh.clean(ctx)
	s.Require().NoError(err)

	s.Require().Zero(s.storageFloat32.Len(ctx))
	s.Require().Zero(s.storageFloat64.Len(ctx))
	s.Require().Zero(s.storageInt32.Len(ctx))
	s.Require().Zero(s.storageInt64.Len(ctx))
	s.Require().Zero(s.storageStr.Len(ctx))
}

func (s *shardTestSuite) TestClean_CtxCancelled() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.sh.clean(ctx)
	s.Require().Error(err)
	s.Require().ErrorContains(err, "context canceled")
}
