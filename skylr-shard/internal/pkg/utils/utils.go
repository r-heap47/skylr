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
