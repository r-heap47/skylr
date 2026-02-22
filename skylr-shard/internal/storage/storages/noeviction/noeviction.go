package noeviction

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/errors"
	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
)

// noeviction - key-value storage without eviction
type noeviction struct {
	store map[string]storage.Entry
	mu    *sync.RWMutex

	curTime         utils.Provider[time.Time]
	cleanupTimeout  utils.Provider[time.Duration]
	cleanupCooldown utils.Provider[time.Duration]

	start <-chan struct{}
}

// Config - noeviction storage config
type Config struct {
	CurTime         utils.Provider[time.Time]
	CleanupTimeout  utils.Provider[time.Duration]
	CleanupCooldown utils.Provider[time.Duration]
	Start           <-chan struct{}
}

// New returns new noeviction storage
func New(cfg Config) storage.Storage {
	noev := &noeviction{
		store:           make(map[string]storage.Entry),
		mu:              &sync.RWMutex{},
		curTime:         cfg.CurTime,
		cleanupTimeout:  cfg.CleanupTimeout,
		cleanupCooldown: cfg.CleanupCooldown,
		start:           cfg.Start,
	}

	// Запускаем cleanup loop в фоне
	go noev.cleanupLoop()

	return noev
}

func (s *noeviction) Get(ctx context.Context, k string) (*storage.Entry, error) {
	if err := utils.CtxDone(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.store[k]
	if !ok {
		return nil, errors.ErrNotFound
	}
	if now := s.curTime(ctx); now.After(entry.Exp) {
		return nil, errors.ErrNotFound
	}

	return &entry, nil
}

func (s *noeviction) Set(ctx context.Context, e storage.Entry) (*storage.Entry, error) {
	if err := utils.CtxDone(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[e.K] = e

	return &e, nil
}

func (s *noeviction) Delete(ctx context.Context, k string) (bool, error) {
	if err := utils.CtxDone(ctx); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, existed := s.store[k]
	delete(s.store, k)

	return existed, nil
}

func (s *noeviction) Clean(ctx context.Context, now time.Time) error {
	// Phase 1: collect expired keys under read-lock so Get is not blocked.
	expired := make([]string, 0)

	err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()

		for k, entry := range s.store {
			if err := utils.CtxDone(ctx); err != nil {
				return err
			}
			if now.After(entry.Exp) {
				expired = append(expired, k)
			}
		}

		return nil
	}()
	if err != nil {
		return fmt.Errorf("Clean: error when collecting expired keys: %w", err)
	}
	if len(expired) == 0 {
		return nil
	}

	// Phase 2: delete under write-lock, re-checking TTL because a client may
	// have called Set on the same key with a fresh TTL between the two phases.

	err = func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		for _, k := range expired {
			if err := utils.CtxDone(ctx); err != nil {
				return err
			}
			if entry, ok := s.store[k]; ok && now.After(entry.Exp) {
				delete(s.store, k)
			}
		}

		return nil
	}()
	if err != nil {
		return fmt.Errorf("Clean: error when deleting expired keys: %w", err)
	}

	return nil
}

// cleanupLoop periodically cleanes up expired entries
func (s *noeviction) cleanupLoop() {
	// waiting for shard to start
	<-s.start

	for {
		ctx := context.Background()

		err := s.cleanWithTimeout(ctx)
		if err != nil {
			// TODO: proper logging
			log.Printf("cleanup error: %s\n", err)
		}

		time.Sleep(s.cleanupCooldown(ctx))
	}
}

// cleanWithTimeout executes cleanup with timeout
func (s *noeviction) cleanWithTimeout(ctx context.Context) error {
	timeout := s.cleanupTimeout(ctx)
	now := s.curTime(ctx)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Clean(ctx, now)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
