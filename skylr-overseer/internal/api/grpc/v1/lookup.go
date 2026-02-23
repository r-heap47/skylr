package v1

import (
	"context"
	"errors"
	"fmt"

	"github.com/r-heap47/skylr/skylr-overseer/internal/hashring"
	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Lookup returns the shard address responsible for the given key.
func (i *Implementation) Lookup(ctx context.Context, req *pbovr.LookupRequest) (*pbovr.LookupResponse, error) {
	if err := validateLookupRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	addr, err := i.ovr.Lookup(req.Key)
	if err != nil {
		if errors.Is(err, hashring.ErrEmptyRing) {
			return nil, status.Error(codes.Unavailable, "no shards are registered")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("ovr.Lookup: %s", err))
	}

	return &pbovr.LookupResponse{ShardAddress: addr}, nil
}

func validateLookupRequest(req *pbovr.LookupRequest) error {
	if req.Key == "" {
		return errors.New("key cannot be empty")
	}
	return nil
}
