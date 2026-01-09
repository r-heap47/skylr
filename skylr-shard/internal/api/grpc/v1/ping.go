package v1

import (
	"context"
	"log"

	"google.golang.org/protobuf/types/known/emptypb"
)

// Ping .
func (i *Implementation) Ping(_ context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	log.Println("[INFO] received ping request")
	return &emptypb.Empty{}, nil
}
