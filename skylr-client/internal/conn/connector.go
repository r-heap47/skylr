package conn

import (
	"context"

	pbshard "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-shard"
)

// Connector obtains ShardClient for a given shard address.
type Connector interface {
	// Connect returns a ShardClient and a release function. Caller must call release when done.
	Connect(ctx context.Context, shardAddr string) (client pbshard.ShardClient, release func(), err error)
}
