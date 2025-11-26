package impl

import (
	"context"
	"sync"
	"time"

	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
)

// noeviction - key-value storage without eviction
type noeviction[T storage.Storable] struct {
	store map[string]storage.Entry[T]
	mu    *sync.RWMutex

	curTime utils.Provider[time.Time]
	clnDur  utils.Provider[time.Duration] // cooldown between cleanups

	start, done <-chan struct{}
}

// Config - noeviction storage config
type Config struct {
	CurTime     utils.Provider[time.Time]
	ClnDur      utils.Provider[time.Duration]
	Done, Start <-chan struct{}
}

// New returns new noeviction storage
func New[T storage.Storable](cfg Config) (storage.Storage[T], error) {
	new := &noeviction[T]{
		store:   make(map[string]storage.Entry[T]),
		mu:      &sync.RWMutex{},
		curTime: cfg.CurTime,
		clnDur:  cfg.ClnDur,
		done:    cfg.Done,
		start:   cfg.Start,
	}

	go new.cleanup()

	return new, nil
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

// cleanup initializes cleaning process
func (s *noeviction[T]) cleanup() {
	// wait for shard initialization
	<-s.start

	for {
		select {
		case <-s.done:
			return
		default:
			s.clean(s.curTime(nil))
		}

		// wait until next clean() invocation
		time.Sleep(s.clnDur(nil))
	}
}

// clean deletes expired keys from storage
func (s *noeviction[T]) clean(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, entry := range s.store {
		if now.After(entry.Exp) {
			delete(s.store, k)
		}
	}
}
