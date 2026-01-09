package v1

import (
	"context"
	"errors"
	"fmt"

	pbshard "github.com/cutlery47/skylr/skylr-shard/internal/pb/skylr-shard"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Get returns Entry by provided key
func (i *Implementation) Get(ctx context.Context, req *pbshard.GetRequest) (*pbshard.GetResponse, error) {
	if err := validateGetRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	entry, err := i.shard.Get(ctx, req.Key)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("shard.Get: %s", err))
	}

	return &pbshard.GetResponse{
		Entry: entry,
	}, nil
}

func validateGetRequest(req *pbshard.GetRequest) error {
	if req == nil {
		return errors.New("UNEXPECTED: GetRequest is nil")
	}
	if req.Key == "" {
		return errors.New("key cannot be empty")
	}

	return nil
}
