package shard

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage/storages/noeviction"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
)

var (
	defaultNow      = time.Now()
	defaultTimeout  = 5 * time.Second
	defaultCooldown = 5 * time.Second
)

type shardTestSuite struct {
	now *time.Time

	nowCalled atomic.Int64

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

	s.nowCalled = atomic.Int64{}

	curTime := func(_ context.Context) time.Time {
		s.nowCalled.Add(1)
		return *s.now
	}

	cleanupTimeout := func(_ context.Context) time.Duration {
		return defaultTimeout
	}

	cleanupCooldown := func(_ context.Context) time.Duration {
		return defaultCooldown
	}

	storage := noeviction.New(noeviction.Config{
		CurTime:         curTime,
		CleanupTimeout:  cleanupTimeout,
		CleanupCooldown: cleanupCooldown,
		Start:           startCh,
	})

	s.storage = storage

	shard := New(Config{
		Storage: storage,
		CurTime: curTime,
	})

	s.sh = shard
}
