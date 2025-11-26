package lfu

import (
	"errors"

	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
)

// New returns new LFU storage
func New[T storage.Storable]() (storage.Storage[T], error) {
	return nil, errors.New("not implemented")
}
