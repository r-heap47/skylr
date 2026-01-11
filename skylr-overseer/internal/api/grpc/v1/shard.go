package v1

import (
	"context"
	"errors"
	"fmt"

	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (i *Implementation) Shard(ctx context.Context, req *pbovr.ShardRequest) (*pbovr.ShardResponse, error) {
	if err := validateShardRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	addr, err := i.ovr.Shard(ctx, req.Key)
	if err != nil {
		return nil, fmt.Errorf("ovr.Shard: %w", err)
	}

	return &pbovr.ShardResponse{
		Address: *addr,
	}, nil
}

func validateShardRequest(req *pbovr.ShardRequest) error {
	if req == nil {
		return errors.New("request shouldn't be nil")
	}
	if req.Key == "" {
		return errors.New("key must be provided")
	}

	return nil
}
