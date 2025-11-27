package app

import (
	"context"
	"errors"
	"fmt"

	pbshard "github.com/cutlery47/skylr/skylr-shard/pkg/pb/skylr-shard"
)

// Get returns Entry by provided key
func (i *Implementation) Get(ctx context.Context, req *pbshard.GetRequest) (*pbshard.GetResponse, error) {
	if err := validateGetRequest(req); err != nil {
		return nil, err
	}

	entry, err := i.shard.Get(ctx, req.Key)
	if err != nil {
		return nil, fmt.Errorf("shard.Get: %w", err)
	}

	return &pbshard.GetResponse{
		Entry: entry,
	}, nil
}

func validateGetRequest(req *pbshard.GetRequest) error {
	if req.Key == "" {
		return errors.New("key cannot be empty")
	}

	return nil
}
