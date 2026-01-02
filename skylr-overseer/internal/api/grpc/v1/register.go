package v1

import (
	"context"
	"errors"
	"fmt"

	pbovr "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	pbshard "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (i *Implementation) Register(ctx context.Context, req *pbovr.RegisterRequest) (*emptypb.Empty, error) {
	if err := validateRegisterRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// check if provided address is reachable
	shardConn, err := grpc.NewClient(
		req.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("couldn't create shard conn: %s", err))
	}

	shardClient := pbshard.NewShardClient(shardConn)

	// check if shard is pingable
	_, err = shardClient.Ping(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("couldn't ping shard: %s", err))
	}

	err = i.ovr.Register(ctx, req.Address)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("ovr.Register: %s", err))
	}

	return &emptypb.Empty{}, nil
}

func validateRegisterRequest(req *pbovr.RegisterRequest) error {
	if req.Address == "" {
		return errors.New("address shoudln't be empty")
	}

	return nil
}
