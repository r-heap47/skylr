package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration to support YAML unmarshalling from strings like "5s".
type Duration struct {
	time.Duration
}

// UnmarshalYAML implements yaml.Unmarshaler for Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}

	d.Duration = parsed

	return nil
}

// Config is the top-level application configuration.
type Config struct {
	GRPC     GRPCConfig     `yaml:"grpc"`
	Overseer OverseerConfig `yaml:"overseer"`
}

// GRPCConfig holds the gRPC server host and port.
type GRPCConfig struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

// OverseerConfig holds timing and threshold settings for the Overseer.
type OverseerConfig struct {
	CheckForShardFailuresDelay Duration `yaml:"check_for_shard_failures_delay"`
	ObserverDelay              Duration `yaml:"observer_delay"`
	ObserverMetricsTimeout     Duration `yaml:"observer_metrics_timeout"`
	ObserverErrorThreshold     int      `yaml:"observer_error_threshold"`
	// VirtualNodesPerShard is the number of virtual nodes placed on the consistent
	// hash ring per physical shard. Higher values improve key distribution at the
	// cost of more memory. Defaults to 150 when not set.
	VirtualNodesPerShard int `yaml:"virtual_nodes_per_shard"`
}

// Load reads and parses the YAML config file at the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) // nolint: gosec
	if err != nil {
		return nil, fmt.Errorf("os.ReadFile: %w", err)
	}

	var cfg Config
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("yaml.Unmarshal: %w", err)
	}

	return &cfg, nil
}
