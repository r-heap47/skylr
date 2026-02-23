package v1

import (
	"context"
	"errors"
	"fmt"

	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Provision creates a new shard via the provisioner and returns its address when registered.
func (i *Implementation) Provision(ctx context.Context, _ *emptypb.Empty) (*pbovr.ProvisionResponse, error) {
	if i.provisioner == nil {
		return nil, status.Error(codes.Unimplemented, "provisioner is not configured")
	}

	addr, err := i.provisioner.Provision(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("provision: %s", err))
	}

	return &pbovr.ProvisionResponse{Address: addr}, nil
}

// Deprovision removes the shard at the given address from the ring and stops it.
func (i *Implementation) Deprovision(ctx context.Context, req *pbovr.DeprovisionRequest) (*emptypb.Empty, error) {
	if i.provisioner == nil {
		return nil, status.Error(codes.Unimplemented, "provisioner is not configured")
	}

	if err := validateDeprovisionRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := i.ovr.Unregister(req.Address); err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("unregister: %s", err))
	}

	if err := i.provisioner.Deprovision(ctx, req.Address); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("deprovision: %s", err))
	}

	return &emptypb.Empty{}, nil
}

func validateDeprovisionRequest(req *pbovr.DeprovisionRequest) error {
	if req == nil || req.Address == "" {
		return errors.New("address must not be empty")
	}
	return nil
}
