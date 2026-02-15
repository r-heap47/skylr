package storage

import (
	"context"
	"time"
)

// Storage - abstraction over key-value storage
type Storage interface {
	// Get returns entry by key k from the storage
	Get(ctx context.Context, k string) (*Entry, error)
	// Set creates/updates key-value pair in the storage
	Set(ctx context.Context, e Entry) (*Entry, error)
	// Delete removes entry by key k from the storage
	// Returns true if the key existed, false otherwise
	Delete(ctx context.Context, k string) (bool, error)
	// Clean cleans up expired entries
	Clean(ctx context.Context, now time.Time) error
	// Len returns the amount of elements in storage
	Len(ctx context.Context) (int, error)
}

// Entry - key-value pair with additional data
type Entry struct {
	K string
	V any
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
