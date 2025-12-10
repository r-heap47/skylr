package app

import (
	"context"
	"errors"
	"fmt"

	pbshard "github.com/cutlery47/skylr/skylr-shard/pkg/pb/skylr-shard"
)

func (i *Implementation) Set(ctx context.Context, req *pbshard.SetRequest) (*pbshard.SetResponse, error) {
	if err := validateSetRequest(req); err != nil {
		return nil, err
	}

	err := i.shard.Set(ctx, req.Input)
	if err != nil {
		return nil, fmt.Errorf("shard.Set: %w", err)
	}

	return &pbshard.SetResponse{}, nil
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

	return nil
}
