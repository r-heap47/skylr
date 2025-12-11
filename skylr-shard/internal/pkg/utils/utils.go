// nolint: revive
package utils

import (
	"context"
)

// CtxDone checks if context is done
func CtxDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil
}

// Provider - value provider for type T
type Provider[T any] func(ctx context.Context) T

// Const - constant value provider
func Const[T any](v T) Provider[T] {
	return func(_ context.Context) T {
		return v
	}
}
