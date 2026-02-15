package shard

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/testutils"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage/storages/noeviction"
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

	storage storage.Storage

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

	storage := noeviction.New(noeviction.Config{
		CurTime: curTime,
	})

	s.storage = storage

	shard := New(Config{
		Storage:         storage,
		CurTime:         curTime,
		CleanupTimeout:  cleanupTimeout,
		CleanupCooldown: cleanupCooldown,
		Start:           startCh,
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

	_, err := s.storage.Set(ctx, storage.Entry{
		K:   "key1",
		V:   float32(1),
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storage.Set(ctx, storage.Entry{
		K:   "key2",
		V:   float64(1),
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storage.Set(ctx, storage.Entry{
		K:   "key3",
		V:   int32(1),
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storage.Set(ctx, storage.Entry{
		K:   "key4",
		V:   int64(1),
		Exp: exp,
	})
	s.Require().NoError(err)

	_, err = s.storage.Set(ctx, storage.Entry{
		K:   "key5",
		V:   "val",
		Exp: exp,
	})
	s.Require().NoError(err)

	s.now = lo.ToPtr(exp.Add(time.Microsecond))

	err = s.sh.clean(ctx)
	s.Require().NoError(err)

	length, err := s.storage.Len(ctx)
	s.Require().NoError(err)
	s.Require().Zero(length)
}

func (s *shardTestSuite) TestClean_CtxCancelled() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.sh.clean(ctx)
	s.Require().Error(err)
	s.Require().ErrorContains(err, "context canceled")
}
