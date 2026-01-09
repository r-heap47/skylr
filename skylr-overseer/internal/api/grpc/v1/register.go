package v1

import (
	"context"
	"errors"
	"fmt"

	pbovr "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Register registers a new shard on overseer
func (i *Implementation) Register(ctx context.Context, req *pbovr.RegisterRequest) (*emptypb.Empty, error) {
	if err := validateRegisterRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err := i.ovr.Register(ctx, req.Address)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("ovr.Register: %s", err))
	}

	return &emptypb.Empty{}, nil
}

func validateRegisterRequest(req *pbovr.RegisterRequest) error {
	// TODO: proper validation
	if req.Address == "" {
		return errors.New("address shoudln't be empty")
	}

	return nil
}
