package v1

import (
	"context"
	"log"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (i *Implementation) Ping(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	log.Println("[INFO] received ping request")
	return &emptypb.Empty{}, nil
}
