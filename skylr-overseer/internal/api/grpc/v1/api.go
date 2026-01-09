package v1

import (
	"github.com/cutlery47/skylr/skylr-overseer/internal/overseer"
	pbovr "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-overseer"
)

// Implementation - grpc service implementation
type Implementation struct {
	pbovr.UnimplementedOverseerServer

	ovr *overseer.Overseer
}

// Config - implementation config
type Config struct {
	Ovr *overseer.Overseer
}

// New creates a new Implementation
func New(cfg *Config) *Implementation {
	return &Implementation{
		ovr: cfg.Ovr,
	}
}
