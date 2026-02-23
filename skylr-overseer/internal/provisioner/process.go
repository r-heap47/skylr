package provisioner

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// ProcessConfig holds configuration for ProcessProvisioner.
type ProcessConfig struct {
	BinaryPath      string
	ConfigPath      string
	OverseerAddress string
	GRPCHost        string
	GRPCPortMin     int
	GRPCPortMax     int
	MaxShards       int
	// ShardCount returns the total number of shards registered on the overseer.
	ShardCount func() int
	// IsShardRegistered returns true when the shard at addr has registered on the overseer.
	IsShardRegistered func(addr string) bool
	// RegistrationPollInterval is how often to poll for shard registration.
	RegistrationPollInterval time.Duration
	// RegistrationTimeout is the max time to wait for a shard to register.
	RegistrationTimeout time.Duration
}

// ProcessProvisioner provisions shards by executing the shard binary as a subprocess.
type ProcessProvisioner struct {
	cfg ProcessConfig

	mu        sync.Mutex
	processes map[string]*exec.Cmd // addr -> cmd
	portUsed  map[int]struct{}     // grpc ports in use
}

// NewProcess creates a new ProcessProvisioner.
func NewProcess(cfg ProcessConfig) *ProcessProvisioner {
	if cfg.RegistrationPollInterval <= 0 {
		cfg.RegistrationPollInterval = 200 * time.Millisecond
	}
	if cfg.RegistrationTimeout <= 0 {
		cfg.RegistrationTimeout = 60 * time.Second
	}
	return &ProcessProvisioner{
		cfg:       cfg,
		processes: make(map[string]*exec.Cmd),
		portUsed:  make(map[int]struct{}),
	}
}

// Provision creates a new shard process and returns its address when it has registered.
func (p *ProcessProvisioner) Provision(ctx context.Context) (string, error) {
	addr, err := p.provisionExec(ctx)
	if err != nil {
		return "", fmt.Errorf("provisionExec: %w", err)
	}

	// Wait for registration
	deadline := time.Now().Add(p.cfg.RegistrationTimeout)
	ticker := time.NewTicker(p.cfg.RegistrationPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = p.Deprovision(ctx, addr)
			return "", ctx.Err()
		default:
		}

		if p.cfg.IsShardRegistered(addr) {
			log.Printf("[INFO] provisioner: shard %s registered", addr)
			return addr, nil
		}

		if time.Now().After(deadline) {
			_ = p.Deprovision(ctx, addr)
			return "", fmt.Errorf("shard %s did not register within %v", addr, p.cfg.RegistrationTimeout)
		}

		select {
		case <-ctx.Done():
			_ = p.Deprovision(ctx, addr)
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// provisionExec start a new shard process
// returns shard addres
func (p *ProcessProvisioner) provisionExec(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cfg.IsShardRegistered == nil {
		return "", fmt.Errorf("IsShardRegistered callback is required")
	}

	// Check max_shards against total shards on overseer
	if p.cfg.ShardCount != nil && p.cfg.ShardCount() >= p.cfg.MaxShards {
		return "", fmt.Errorf("max shards (%d) reached", p.cfg.MaxShards)
	}

	// Find free port in range
	grpcPort := 0
	for port := p.cfg.GRPCPortMin; port <= p.cfg.GRPCPortMax; port++ {
		if port+1 > p.cfg.GRPCPortMax {
			break
		}
		if _, used := p.portUsed[port]; !used {
			grpcPort = port
			break
		}
	}
	if grpcPort == 0 {
		return "", fmt.Errorf("no free port in range [%d, %d]", p.cfg.GRPCPortMin, p.cfg.GRPCPortMax)
	}

	p.portUsed[grpcPort] = struct{}{}
	gatewayPort := grpcPort + 1

	// building final shard address
	addr := net.JoinHostPort(p.cfg.GRPCHost, strconv.Itoa(grpcPort))

	args := []string{
		"-config", p.cfg.ConfigPath,
		"-grpc-host", p.cfg.GRPCHost,
		"-grpc-port", strconv.Itoa(grpcPort),
		"-gateway-host", p.cfg.GRPCHost,
		"-gateway-port", strconv.Itoa(gatewayPort),
		"-overseer", p.cfg.OverseerAddress,
	}

	cmd := exec.CommandContext(ctx, p.cfg.BinaryPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		delete(p.portUsed, grpcPort)
		return "", fmt.Errorf("start shard: %w", err)
	}

	p.processes[addr] = cmd
	return addr, nil
}

// Deprovision stops the shard process at addr.
func (p *ProcessProvisioner) Deprovision(ctx context.Context, addr string) error {
	p.mu.Lock()
	cmd, ok := p.processes[addr]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("shard %q not found", addr)
	}
	delete(p.processes, addr)

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("invalid addr %q: %w", addr, err)
	}
	_ = host
	grpcPort, err := strconv.Atoi(portStr)
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("invalid port in addr %q: %w", addr, err)
	}
	delete(p.portUsed, grpcPort)
	p.mu.Unlock()

	if cmd.Process == nil {
		return nil
	}

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		return nil
	}

	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		return ctx.Err()
	case <-done:
		return nil
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		return nil
	}
}
