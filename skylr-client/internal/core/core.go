package core

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/r-heap47/skylr/skylr-client/internal/conn"
	pbovr "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-overseer"
	pbshard "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-shard"
)

// Client implements the skylr storage client.
type Client struct {
	ovrConn   *grpc.ClientConn
	ovrClient pbovr.OverseerClient
	conn      conn.Connector
	timeout   time.Duration
}

// Config configures Client.
type Config struct {
	Conn    conn.Connector
	Timeout time.Duration
}

// New creates a new Client.
func New(ctx context.Context, overseerAddr string, cfg Config) (*Client, error) {
	//nolint:staticcheck // SA1019: DialContext supports ctx cancellation; NewClient does not
	ovrConn, err := grpc.DialContext(ctx, overseerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to overseer: %w", err)
	}

	shardPool := cfg.Conn
	if shardPool == nil {
		shardPool = conn.NewDialConnector()
	}

	return &Client{
		ovrConn:   ovrConn,
		ovrClient: pbovr.NewOverseerClient(ovrConn),
		conn:      shardPool,
		timeout:   cfg.Timeout,
	}, nil
}

// Get fetches an entry by key.
func (c *Client) Get(ctx context.Context, key string) (*pbshard.Entry, error) {
	shardAddr, err := c.lookup(ctx, key)
	if err != nil {
		return nil, err
	}

	client, release, err := c.conn.Connect(ctx, shardAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to shard: %w", err)
	}
	defer release()

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := client.Get(ctx, &pbshard.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}

	return resp.Entry, nil
}

// Set stores a key-value pair with TTL.
func (c *Client) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	shardAddr, err := c.lookup(ctx, key)
	if err != nil {
		return err
	}

	client, release, err := c.conn.Connect(ctx, shardAddr)
	if err != nil {
		return fmt.Errorf("connect to shard: %w", err)
	}
	defer release()

	entry, err := valueToEntry(key, value)
	if err != nil {
		return err
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err = client.Set(ctx, &pbshard.SetRequest{
		Input: &pbshard.InputEntry{
			Entry: entry,
			Ttl:   durationpb.New(ttl),
		},
	})
	return err
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	shardAddr, err := c.lookup(ctx, key)
	if err != nil {
		return false, err
	}

	client, release, err := c.conn.Connect(ctx, shardAddr)
	if err != nil {
		return false, fmt.Errorf("connect to shard: %w", err)
	}
	defer release()

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := client.Delete(ctx, &pbshard.DeleteRequest{Key: key})
	if err != nil {
		return false, err
	}

	return resp.Deleted, nil
}

// Close releases resources.
func (c *Client) Close() error {
	return c.ovrConn.Close()
}

func (c *Client) lookup(ctx context.Context, key string) (string, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.ovrClient.Lookup(ctx, &pbovr.LookupRequest{Key: key})
	if err != nil {
		return "", fmt.Errorf("lookup: %w", err)
	}
	if resp.ShardAddress == "" {
		return "", fmt.Errorf("lookup: empty shard address")
	}

	return resp.ShardAddress, nil
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout > 0 {
		return context.WithTimeout(ctx, c.timeout)
	}

	return ctx, func() {}
}

func valueToEntry(key string, v interface{}) (*pbshard.Entry, error) {
	entry := &pbshard.Entry{Key: key}
	switch val := v.(type) {
	case string:
		entry.Value = &pbshard.Entry_ValueStr{ValueStr: val}
	case int32:
		entry.Value = &pbshard.Entry_ValueInt32{ValueInt32: val}
	case int64:
		entry.Value = &pbshard.Entry_ValueInt64{ValueInt64: val}
	case float32:
		entry.Value = &pbshard.Entry_ValueFloat{ValueFloat: val}
	case float64:
		entry.Value = &pbshard.Entry_ValueDouble{ValueDouble: val}
	default:
		return nil, fmt.Errorf("unsupported value type: %T (use string, int32, int64, float32, float64)", v)
	}
	return entry, nil
}
