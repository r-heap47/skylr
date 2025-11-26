package noeviction

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

	start, done <-chan struct{}

	curTime utils.Provider[time.Time]
	clnDur  utils.Provider[time.Duration] // cooldown between cleanups
}

// Config - noeviction storage config
type Config struct {
	CurTime     utils.Provider[time.Time]
	ClnDur      utils.Provider[time.Duration]
	Done, Start <-chan struct{}
}

// New returns new noeviction storage
func New[T storage.Storable](cfg Config) (storage.Storage[T], error) {
	noev := &noeviction[T]{
		store:   make(map[string]storage.Entry[T]),
		mu:      &sync.RWMutex{},
		curTime: cfg.CurTime,
		clnDur:  cfg.ClnDur,
		done:    cfg.Done,
		start:   cfg.Start,
	}

	go noev.cleanup()

	return noev, nil
}

func (s *noeviction[T]) Get(ctx context.Context, k string) (*storage.Entry[T], error) {
	if utils.CtxDone(ctx) {
		return nil, storage.ErrCtxDone
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.store[k]
	if !ok {
		return nil, storage.ErrNotFound
	}
	if now := s.curTime(ctx); now.After(entry.Exp) {
		delete(s.store, k)
		return nil, storage.ErrNotFound
	}

	return &entry, nil
}

func (s *noeviction[T]) Set(ctx context.Context, e storage.Entry[T]) (*storage.Entry[T], error) {
	if utils.CtxDone(ctx) {
		return nil, storage.ErrCtxDone
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[e.K] = e

	return &e, nil
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
