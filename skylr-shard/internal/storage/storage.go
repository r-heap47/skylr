package storage

import (
	"context"
	"errors"
	"time"
)

// Storable - set of types, that can be stored as values in the storage
type Storable interface {
	string | int32 | int64 | float32 | float64
}

// Storage - abstraction over key-value storage
type Storage[T Storable] interface {
	// Get returns entry by key k from the storage
	Get(ctx context.Context, k string) (*Entry[T], error)
	// Set creates/updates key-value pair in the storage
	Set(ctx context.Context, e Entry[T]) (*Entry[T], error)
}

// Entry - key-value pair with additional data
type Entry[T Storable] struct {
	K string
	V T
	// expiration time
	Exp time.Time
}

// EvictionPolicy - policy, which determines which keys should be evicted from the storage
type EvictionPolicy int

const (
	// NoEviction - eviction turned off
	NoEviction EvictionPolicy = iota
	// LRU - Least Recently Used policy
	LRU
	// LFU - Least Frequently Used policy
	LFU
	// Random - evict random keys
	Random
)

var (
	// ErrNotFound - error, which signifies that provided key was not found in the storage
	ErrNotFound = errors.New("not found")
	// ErrCtxDone - error, which signifies that provided ctx was done
	ErrCtxDone = errors.New("ctx is done")
)
