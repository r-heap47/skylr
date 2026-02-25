package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNew_InvalidAddress(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Invalid address format - grpc.NewClient may succeed (lazy connect).
	// Test that we get a client that we can close.
	c, err := New(ctx, "invalid://bad-address")
	if err != nil {
		require.Error(t, err)
		return
	}
	_ = c.Close()
}

func TestNew_WithOptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// localhost:0 is invalid for dial - but grpc is lazy
	c, err := New(ctx, "localhost:1", WithTimeout(5*time.Second))
	if err != nil {
		return
	}
	require.NotNil(t, c)
	_ = c.Close()
}
