package v1

import (
	"time"

	"github.com/r-heap47/skylr/skylr-shard/internal/metrics"
	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-shard/internal/shard"
)

// Implementation - grpc service implementation
type Implementation struct {
	pbshard.UnimplementedShardServer

	shard     *shard.Shard
	collector *metrics.Collector

	metricsDelay utils.Provider[time.Duration]
}

// Config - API implementation config
type Config struct {
	Shard        *shard.Shard
	Collector    *metrics.Collector
	MetricsDelay utils.Provider[time.Duration]
}

// New creates new API implementation
func New(cfg Config) *Implementation {
	return &Implementation{
		shard:        cfg.Shard,
		collector:    cfg.Collector,
		metricsDelay: cfg.MetricsDelay,
	}
}
