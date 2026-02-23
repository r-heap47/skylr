package v1

import (
	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"

	"github.com/r-heap47/skylr/skylr-overseer/internal/overseer"
	"github.com/r-heap47/skylr/skylr-overseer/internal/provisioner"
)

// Implementation - grpc service implementation
type Implementation struct {
	pbovr.UnimplementedOverseerServer

	ovr         *overseer.Overseer
	provisioner provisioner.ShardProvisioner
}

// Config - implementation config
type Config struct {
	Ovr         *overseer.Overseer
	Provisioner provisioner.ShardProvisioner
}

// New creates a new Implementation
func New(cfg *Config) *Implementation {
	return &Implementation{
		ovr:         cfg.Ovr,
		provisioner: cfg.Provisioner,
	}
}
