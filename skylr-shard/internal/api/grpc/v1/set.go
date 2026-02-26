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
	"google.golang.org/protobuf/types/known/emptypb"
)

// Set uploads new entry to storage
func (i *Implementation) Set(ctx context.Context, req *pbshard.SetRequest) (*emptypb.Empty, error) {
	defer metrics.IncSetOps()

	if err := validateSetRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err := i.shard.Set(ctx, req.Input)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("shard.Set: %s", err))
	}
	if i.logSetDelete(ctx) {
		log.Printf("[SET] key = %s", req.Input.Entry.Key)
	}

	return &emptypb.Empty{}, nil
}

func validateSetRequest(req *pbshard.SetRequest) error {
	if req == nil {
		return errors.New("UNEXPECTED: SetRequest is nil")
	}
	if req.Input == nil {
		return errors.New("input cannot be nil")
	}
	if req.Input.Entry == nil {
		return errors.New("entry cannot be nil")
	}
	if req.Input.Entry.Key == "" {
		return errors.New("key cannot be empty")
	}
	if req.Input.Entry.Value == nil {
		return errors.New("value cannot be nil")
	}
	if req.Input.Ttl == nil {
		return errors.New("ttl cannot be nil")
	}
	if req.Input.Ttl.AsDuration() < 0 {
		return errors.New("ttl cannot be negative")
	}

	return nil
}
