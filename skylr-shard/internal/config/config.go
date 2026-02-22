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
	Gateway  GatewayConfig  `yaml:"gateway"`
	Storage  StorageConfig  `yaml:"storage"`
	Overseer OverseerConfig `yaml:"overseer"`
	Graceful bool           `yaml:"graceful"`
}

// GRPCConfig holds the gRPC server host and port.
type GRPCConfig struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

// GatewayConfig holds the HTTP gateway server settings.
type GatewayConfig struct {
	Host            string   `yaml:"host"`
	Port            string   `yaml:"port"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	IdleTimeout     Duration `yaml:"idle_timeout"`
}

// StorageConfig holds the noeviction storage timing settings.
type StorageConfig struct {
	CleanupTimeout  Duration `yaml:"cleanup_timeout"`
	CleanupCooldown Duration `yaml:"cleanup_cooldown"`
}

// OverseerConfig holds the Overseer gRPC address.
type OverseerConfig struct {
	Address string `yaml:"address"`
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
