package v1

import (
	"context"
	stderrors "errors"
	"fmt"

	"github.com/r-heap47/skylr/skylr-shard/internal/metrics"
	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Get returns Entry by provided key
func (i *Implementation) Get(ctx context.Context, req *pbshard.GetRequest) (*pbshard.GetResponse, error) {
	defer metrics.IncGetOps()

	if err := validateGetRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	entry, err := i.shard.Get(ctx, req.Key)
	if err != nil {
		if stderrors.Is(err, errors.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "entry by given key not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("shard.Get: %s", err))
	}

	return &pbshard.GetResponse{
		Entry: entry,
	}, nil
}

func validateGetRequest(req *pbshard.GetRequest) error {
	if req == nil {
		return stderrors.New("UNEXPECTED: GetRequest is nil")
	}
	if req.Key == "" {
		return stderrors.New("key cannot be empty")
	}

	return nil
}
