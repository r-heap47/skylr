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

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}

	d.Duration = parsed

	return nil
}

type Config struct {
	GRPC     GRPCConfig     `yaml:"grpc"`
	Gateway  GatewayConfig  `yaml:"gateway"`
	Storage  StorageConfig  `yaml:"storage"`
	Overseer OverseerConfig `yaml:"overseer"`
	Graceful bool           `yaml:"graceful"`
}

type GRPCConfig struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

type GatewayConfig struct {
	Host            string   `yaml:"host"`
	Port            string   `yaml:"port"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	IdleTimeout     Duration `yaml:"idle_timeout"`
}

type StorageConfig struct {
	CleanupTimeout  Duration `yaml:"cleanup_timeout"`
	CleanupCooldown Duration `yaml:"cleanup_cooldown"`
}

type OverseerConfig struct {
	Address string `yaml:"address"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("os.ReadFile: %w", err)
	}

	var cfg Config
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("yaml.Unmarshal: %w", err)
	}

	return &cfg, nil
}
