package api

import (
	"github.com/cutlery47/skylr/skylr-shard/internal/shard"
	pbshard "github.com/cutlery47/skylr/skylr-shard/internal/pb/skylr-shard"
)

// Implementation - grpc service implementation
type Implementation struct {
	pbshard.UnimplementedShardServer

	shard *shard.Shard
}

type Config struct {
	Shard *shard.Shard
}

func New(cfg Config) *Implementation {
	return &Implementation{
		shard: cfg.Shard,
	}
}
