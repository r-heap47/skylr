package lfu

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
)

type lfu struct{}

// New returns new LFU storage
// TODO: impl
func New() (storage.Storage, error) {
	return &lfu{}, nil
}

func (l *lfu) Get(_ context.Context, _ string) (*storage.Entry, error) {
	return nil, errors.New("not implemented")
}

func (l *lfu) Set(_ context.Context, _ storage.Entry) (*storage.Entry, error) {
	return nil, errors.New("not implemented")
}

func (l *lfu) Delete(_ context.Context, _ string) (bool, error) {
	return false, errors.New("not implemented")
}

func (l *lfu) Clean(_ context.Context, _ time.Time) error {
	return errors.New("not implemented")
}

func (l *lfu) Scan(_ context.Context) iter.Seq2[*storage.Entry, error] {
	return func(yield func(*storage.Entry, error) bool) {
		yield(nil, errors.New("not implemented"))
	}
}

func (l *lfu) Len(_ context.Context) (int, error) {
	return 0, errors.New("not implemented")
}
