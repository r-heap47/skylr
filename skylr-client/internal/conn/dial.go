package conn

import (
	"context"

	pbshard "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-shard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialConnector creates a new gRPC connection to the shard for each Connect call.
// Caller must call release() when done to close the connection.
type DialConnector struct{}

// NewDialConnector returns a Connector that dials the shard on each Connect.
func NewDialConnector() *DialConnector {
	return &DialConnector{}
}

// Connect dials the shard and returns a ShardClient. release() closes the connection.
func (d *DialConnector) Connect(ctx context.Context, shardAddr string) (pbshard.ShardClient, func(), error) {
	conn, err := grpc.NewClient(shardAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return pbshard.NewShardClient(conn), func() { _ = conn.Close() }, nil
}
