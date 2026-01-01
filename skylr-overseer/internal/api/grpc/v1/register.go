package v1

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (i *Implementation) Register(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "peer info not found")
	}

	err := i.ovr.Register(ctx, p.Addr.String())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("ovr.Register: %s", err))
	}

	return &emptypb.Empty{}, nil
}
