// nolint: revive
package utils

import (
	"context"
)

// CtxDone checks if context is done
func CtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}

	return false
}

// Provider - value provider for type T
type Provider[T any] func(ctx context.Context) T

// Const - constant value provider
func Const[T any](v T) Provider[T] {
	return func(_ context.Context) T {
		return v
	}
}
