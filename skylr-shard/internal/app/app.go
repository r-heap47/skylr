package app

import (
	"github.com/cutlery47/skylr/skylr-shard/internal/shard"
	pbshard "github.com/cutlery47/skylr/skylr-shard/pkg/pb/skylr-shard"
)

// Implementation - grpc service implementation
type Implementation struct {
	pbshard.UnimplementedShardServer

	shard shard.Shard
}
