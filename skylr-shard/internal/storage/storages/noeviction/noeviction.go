package impl

import (
	"context"
	"sync"

	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
)

// noeviction - key-value storage without eviction
type noeviction[T storage.Storable] struct {
	store map[string]storage.Entry[T]
	mu    *sync.RWMutex
}

// New returns new noeviction storage
func New[T storage.Storable]() (storage.Storage[T], error) {
	return &noeviction[T]{
		store: make(map[string]storage.Entry[T]),
		mu:    &sync.RWMutex{},
	}, nil
}

func (s *noeviction[T]) Get(ctx context.Context, k string) (*storage.Entry[T], error) {
	if utils.CtxDone(ctx) {
		return nil, storage.ErrCtxDone
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.store[k]
	if !ok {
		return nil, storage.ErrNotFound
	}

	return &v, nil
}

func (s *noeviction[T]) Set(ctx context.Context, e storage.Entry[T]) error {
	if utils.CtxDone(ctx) {
		return storage.ErrCtxDone
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[e.K] = e

	return nil
}

func (s *noeviction[T]) Size(ctx context.Context) (int, error) {
	if utils.CtxDone(ctx) {
		return 0, storage.ErrCtxDone
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.store), nil
}
