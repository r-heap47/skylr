package storage

import "context"

// Storage - интерфейс хранилища
type Storage interface {
	// GetString возвращает строку по ключу k
	GetString(ctx context.Context, k string) (string, error)
	// SetString устанавливает строку v в соответствие ключу k
	SetString(ctx context.Context, k, v string) error
	// GetInt64 возвращает int64 по ключу k
	GetInt64(ctx context.Context, k string) (int64, error)
	// SetInt64 устанавливает int64 v в соответствие ключу k
	SetInt64(ctx context.Context, k string, v int64) error
	// GetFloat64 возвращает float64 по ключу k
	GetFloat64(ctx context.Context, k string) (float64, error)
	// SetFloat64 устанавливает float64 v в соответствие ключу k
	SetFloat64(ctx context.Context, k string, v float64) error
}
