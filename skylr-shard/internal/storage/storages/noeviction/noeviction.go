package noeviction

import (
	"context"
	"sync"
	"time"

	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/errors"
	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
)

// noeviction - key-value storage without eviction
type noeviction[T storage.Storable] struct {
	store map[string]storage.Entry[T]
	mu    *sync.RWMutex

	curTime utils.Provider[time.Time]
}

// Config - noeviction storage config
type Config struct {
	CurTime utils.Provider[time.Time]
}

// New returns new noeviction storage
func New[T storage.Storable](cfg Config) storage.Storage[T] {
	noev := &noeviction[T]{
		store:   make(map[string]storage.Entry[T]),
		mu:      &sync.RWMutex{},
		curTime: cfg.CurTime,
	}

	return noev
}

func (s *noeviction[T]) Get(ctx context.Context, k string) (*storage.Entry[T], error) {
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
		delete(s.store, k)
		return nil, errors.ErrNotFound
	}

	return &entry, nil
}

func (s *noeviction[T]) Set(ctx context.Context, e storage.Entry[T]) (*storage.Entry[T], error) {
	if err := utils.CtxDone(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[e.K] = e

	return &e, nil
}

func (s *noeviction[T]) Clean(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, entry := range s.store {
		if err := utils.CtxDone(ctx); err != nil {
			return err
		}

		if now.After(entry.Exp) {
			delete(s.store, k)
		}
	}

	return nil
}

func (s *noeviction[T]) Len(ctx context.Context) (int, error) {
	if err := utils.CtxDone(ctx); err != nil {
		return 0, err
	}

	return len(s.store), nil
}
