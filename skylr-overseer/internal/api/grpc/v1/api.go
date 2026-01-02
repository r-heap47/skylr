package v1

import (
	"github.com/cutlery47/skylr/skylr-overseer/internal/overseer"
	pbovr "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-overseer"
)

type Implementation struct {
	pbovr.UnimplementedOverseerServer

	ovr *overseer.Overseer
}

type Config struct {
	Ovr *overseer.Overseer
}

func New(cfg *Config) *Implementation {
	return &Implementation{
		ovr: cfg.Ovr,
	}
}
