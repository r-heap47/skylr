package api

import (
	pbshard "github.com/cutlery47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/cutlery47/skylr/skylr-shard/internal/shard"
)

// Implementation - grpc service implementation
type Implementation struct {
	pbshard.UnimplementedShardServer

	shard *shard.Shard
}

// Config - API implementation config
type Config struct {
	Shard *shard.Shard
}

// New creates new API implementation
func New(cfg Config) *Implementation {
	return &Implementation{
		shard: cfg.Shard,
	}
}
