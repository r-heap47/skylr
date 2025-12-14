package random

import (
	"errors"

	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
)

// New returns new random-eviction storage
// TODO: impl
func New[T storage.Storable]() (storage.Storage[T], error) {
	return nil, errors.New("not implemented")
}
