package random

import (
	"context"
	"errors"
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
)

type random struct{}

// New returns new random-eviction storage
// TODO: impl
func New() (storage.Storage, error) {
	return &random{}, nil
}

func (r *random) Get(_ context.Context, _ string) (*storage.Entry, error) {
	return nil, errors.New("not implemented")
}

func (r *random) Set(_ context.Context, _ storage.Entry) (*storage.Entry, error) {
	return nil, errors.New("not implemented")
}

func (r *random) Delete(_ context.Context, _ string) (bool, error) {
	return false, errors.New("not implemented")
}

func (r *random) Clean(_ context.Context, _ time.Time) error {
	return errors.New("not implemented")
}

func (r *random) Len(_ context.Context) (int, error) {
	return 0, errors.New("not implemented")
}
