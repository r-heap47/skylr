package lru

import (
	"context"
	"errors"
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
)

type lru struct{}

// New returns new LRU storage
// TODO: impl
func New() (storage.Storage, error) {
	return &lru{}, nil
}

func (l *lru) Get(_ context.Context, _ string) (*storage.Entry, error) {
	return nil, errors.New("not implemented")
}

func (l *lru) Set(_ context.Context, _ storage.Entry) (*storage.Entry, error) {
	return nil, errors.New("not implemented")
}

func (l *lru) Delete(_ context.Context, _ string) (bool, error) {
	return false, errors.New("not implemented")
}

func (l *lru) Clean(_ context.Context, _ time.Time) error {
	return errors.New("not implemented")
}

func (l *lru) Len(_ context.Context) (int, error) {
	return 0, errors.New("not implemented")
}
