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
	GRPC        GRPCConfig        `yaml:"grpc"`
	Overseer    OverseerConfig    `yaml:"overseer"`
	Provisioner ProvisionerConfig `yaml:"provisioner"`
}

// ProvisionerConfig holds provisioner settings. When type is empty, provisioning is disabled.
type ProvisionerConfig struct {
	Type    string                `yaml:"type"`
	Process ProcessProvisionerCfg `yaml:"process"`
}

// ProcessProvisionerCfg holds ProcessProvisioner-specific config.
type ProcessProvisionerCfg struct {
	BinaryPath            string   `yaml:"binary_path"`
	ConfigPath            string   `yaml:"config_path"`
	OverseerAddress       string   `yaml:"overseer_address"`
	GRPCHost              string   `yaml:"grpc_host"`
	GRPCPortMin           int      `yaml:"grpc_port_min"`
	GRPCPortMax           int      `yaml:"grpc_port_max"`
	MaxShards             int      `yaml:"max_shards"`
	InitialShards         int      `yaml:"initial_shards"` // number of shards to provision on boot; 0 = none
	RegistrationTimeout   Duration `yaml:"registration_timeout"`
	PostRegistrationDelay Duration `yaml:"post_registration_delay"`
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
	// LogStorageOnMetrics enables debug logging of all storage entries after each
	// metrics poll. Useful for local development and debugging.
	LogStorageOnMetrics bool `yaml:"log_storage_on_metrics"`
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
