package client

import (
	"time"

	"github.com/r-heap47/skylr/skylr-client/internal/conn"
)

// Option configures the Client.
type Option func(*options)

type options struct {
	timeout time.Duration
	conn    conn.Connector
}

// WithTimeout sets the default timeout for RPC calls.
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		o.timeout = d
	}
}

// WithConnector sets the connector for shard connections. If not set, DialConnector is used.
func WithConnector(p conn.Connector) Option {
	return func(o *options) {
		o.conn = p
	}
}
