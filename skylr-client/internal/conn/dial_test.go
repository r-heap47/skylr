package conn

import (
	"context"
	"testing"
)

func TestDialConnector_Connect_InvalidAddress(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d := NewDialConnector()

	// Invalid address (unparseable) - grpc.NewClient may still succeed (lazy connect).
	// Test that we get a client and release works.
	_, release, err := d.Connect(ctx, "invalid-address://")
	if err != nil {
		// Some grpc versions fail on invalid addresses
		return
	}
	release()
}
