package v1

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/r-heap47/skylr/skylr-shard/internal/metrics"
	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Delete removes entry by provided key
func (i *Implementation) Delete(ctx context.Context, req *pbshard.DeleteRequest) (*pbshard.DeleteResponse, error) {
	defer metrics.IncDeleteOps()

	if err := validateDeleteRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	deleted, err := i.shard.Delete(ctx, req.Key)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("shard.Delete: %s", err))
	}
	if deleted {
		log.Printf("[DELETE] key = %s", req.Key)
	}

	return &pbshard.DeleteResponse{
		Deleted: deleted,
	}, nil
}

func validateDeleteRequest(req *pbshard.DeleteRequest) error {
	if req == nil {
		return errors.New("UNEXPECTED: DeleteRequest is nil")
	}
	if req.Key == "" {
		return errors.New("key cannot be empty")
	}

	return nil
}
