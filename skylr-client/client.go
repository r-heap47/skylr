package client

import (
	"context"
	"time"

	"github.com/r-heap47/skylr/skylr-client/internal/core"
	pbshard "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-shard"
)

// Client is the skylr distributed storage client.
type Client struct {
	impl *core.Client
}

// New creates a new Client. overseerAddr is the gRPC address of the Overseer (e.g. "localhost:9000").
func New(ctx context.Context, overseerAddr string, opts ...Option) (*Client, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	c, err := core.New(ctx, overseerAddr, core.Config{
		Timeout: o.timeout,
		Conn:    o.conn,
	})
	if err != nil {
		return nil, err
	}

	return &Client{impl: c}, nil
}

// Get returns the Entry for the given key. Returns grpc.NotFound if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (*pbshard.Entry, error) {
	return c.impl.Get(ctx, key)
}

// Set stores key-value with TTL. value must be string, int32, int64, float32, or float64.
func (c *Client) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return c.impl.Set(ctx, key, value, ttl)
}

// Delete removes the key. Returns true if the key existed and was deleted.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	return c.impl.Delete(ctx, key)
}

// Close releases resources.
func (c *Client) Close() error {
	return c.impl.Close()
}
