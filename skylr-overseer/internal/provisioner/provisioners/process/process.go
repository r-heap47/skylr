package process

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

	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/logwriter"
)

// Config holds configuration for the process provisioner.
type Config struct {
	BinaryPath      string // Path to the shard binary.
	ConfigPath      string // Path to the shard config file.
	OverseerAddress string // Overseer gRPC address for shard registration.
	GRPCHost        string // Host for shard gRPC and gateway listeners.
	GRPCPortMin     int    // Minimum gRPC port (inclusive).
	GRPCPortMax     int    // Maximum gRPC port (exclusive); gateway uses grpcPort+1.
	MaxShards       int    // Maximum number of shards that can be provisioned.
	// ShardCount returns the total number of shards registered on the overseer.
	ShardCount func() int
	// IsShardRegistered returns true when the shard at addr has registered on the overseer.
	IsShardRegistered func(addr string) bool
	// RegistrationPollInterval is how often to poll for shard registration.
	RegistrationPollInterval time.Duration
	// RegistrationTimeout is the max time to wait for a shard to register.
	RegistrationTimeout time.Duration
	// PostRegistrationDelay is how long to wait after the shard registers before returning.
	PostRegistrationDelay time.Duration
}

// Provisioner provisions shards by executing the shard binary as a subprocess.
type Provisioner struct {
	cfg Config

	mu        sync.Mutex
	processes map[string]*exec.Cmd // addr -> cmd
	portUsed  map[int]struct{}     // grpc + gateway ports in use
}

// New creates a new process provisioner.
func New(cfg Config) *Provisioner {
	// Apply defaults for optional duration fields.
	if cfg.RegistrationPollInterval <= 0 {
		cfg.RegistrationPollInterval = 200 * time.Millisecond
	}
	if cfg.RegistrationTimeout <= 0 {
		cfg.RegistrationTimeout = 60 * time.Second
	}
	if cfg.PostRegistrationDelay < 0 {
		cfg.PostRegistrationDelay = 0
	}
	return &Provisioner{
		cfg:       cfg,
		processes: make(map[string]*exec.Cmd),
		portUsed:  make(map[int]struct{}),
	}
}

// Provision creates a new shard process and returns its address when it has registered.
// Fails if the shard does not register within RegistrationTimeout.
func (p *Provisioner) Provision(ctx context.Context) (string, error) {
	// Start the shard subprocess (allocates ports, runs binary).
	addr, err := p.startShard()
	if err != nil {
		return "", fmt.Errorf("start shard: %w", err)
	}

	// Block until the shard registers with the overseer or timeout/context cancel.
	if err := p.waitForRegistration(ctx, addr); err != nil {
		_ = p.Deprovision(ctx, addr) // clean up process and ports on failure
		return "", err
	}

	return addr, nil
}

// Deprovision stops the shard process at addr.
func (p *Provisioner) Deprovision(ctx context.Context, addr string) error {
	// Remove from tracking and get process handle + port for cleanup.
	cmd, grpcPort, err := p.removeShard(addr)
	if err != nil {
		return err
	}
	p.releasePorts(grpcPort) // free ports for future provisions

	return p.killProcessGroup(ctx, cmd)
}

// Shutdown kills all provisioned shard processes. Call when the overseer exits.
func (p *Provisioner) Shutdown(ctx context.Context) error {
	for _, addr := range p.addrs() {
		_ = p.Deprovision(ctx, addr)
	}

	return nil
}

func (p *Provisioner) addrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	addrs := make([]string, 0, len(p.processes))
	for addr := range p.processes {
		addrs = append(addrs, addr)
	}

	return addrs
}

// startShard allocates ports, starts the shard process, and returns its address.
// Caller must call Deprovision on error or to clean up.
func (p *Provisioner) startShard() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cfg.IsShardRegistered == nil {
		return "", fmt.Errorf("IsShardRegistered callback is required")
	}
	if p.cfg.ShardCount != nil && p.cfg.ShardCount() >= p.cfg.MaxShards {
		return "", fmt.Errorf("max shards (%d) reached", p.cfg.MaxShards)
	}

	// Allocate a pair of ports: grpc and gateway (grpc+1).
	grpcPort, err := p.allocatePort()
	if err != nil {
		return "", err
	}
	gatewayPort := grpcPort + 1
	p.portUsed[grpcPort] = struct{}{}
	p.portUsed[gatewayPort] = struct{}{}

	addr := net.JoinHostPort(p.cfg.GRPCHost, strconv.Itoa(grpcPort))
	args := p.shardArgs(grpcPort, gatewayPort)

	// Run shard as its own process group so we can kill the whole group on Deprovision.
	// #nosec G204 -- BinaryPath and args come from trusted config.
	cmd := exec.Command(p.cfg.BinaryPath, args...)
	cmd.Stdout = logwriter.LinePrefix(fmt.Sprintf("shard %s", addr))
	cmd.Stderr = logwriter.LinePrefix(fmt.Sprintf("shard %s", addr))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	err = cmd.Start()
	if err != nil {
		p.releasePortsLocked(grpcPort) // rollback port allocation
		return "", fmt.Errorf("exec: %w", err)
	}

	p.processes[addr] = cmd
	return addr, nil
}

// removeShard removes the shard from tracking. Returns (cmd, grpcPort, nil) or (nil, 0, err).
// Caller must release ports and kill the process.
func (p *Provisioner) removeShard(addr string) (*exec.Cmd, int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	cmd, ok := p.processes[addr]
	if !ok {
		return nil, 0, fmt.Errorf("shard %q not found", addr)
	}
	delete(p.processes, addr)

	// Extract grpc port from addr (e.g. "localhost:5000" -> 5000) to release both grpc and gateway ports.
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid addr %q: %w", addr, err)
	}
	grpcPort, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid port in addr %q: %w", addr, err)
	}
	return cmd, grpcPort, nil
}

// allocatePort returns a free gRPC port in [GRPCPortMin, GRPCPortMax).
// Caller must hold p.mu. The corresponding gateway port (grpcPort+1) must fit in range.
func (p *Provisioner) allocatePort() (int, error) {
	// Skip last port so gateway (port+1) stays within range.
	for port := p.cfg.GRPCPortMin; port < p.cfg.GRPCPortMax; port++ {
		if _, used := p.portUsed[port]; !used {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no free port in range [%d, %d)", p.cfg.GRPCPortMin, p.cfg.GRPCPortMax)
}

// releasePorts frees both grpcPort and gatewayPort (grpcPort+1) for reuse.
func (p *Provisioner) releasePorts(grpcPort int) {
	p.mu.Lock()
	p.releasePortsLocked(grpcPort)
	p.mu.Unlock()
}

// releasePortsLocked releases ports. Caller must hold p.mu.
func (p *Provisioner) releasePortsLocked(grpcPort int) {
	delete(p.portUsed, grpcPort)
	delete(p.portUsed, grpcPort+1)
}

// shardArgs builds command-line arguments for the shard binary.
func (p *Provisioner) shardArgs(grpcPort, gatewayPort int) []string {
	return []string{
		"-config", p.cfg.ConfigPath,
		"-grpc-host", p.cfg.GRPCHost,
		"-grpc-port", strconv.Itoa(grpcPort),
		"-gateway-host", p.cfg.GRPCHost,
		"-gateway-port", strconv.Itoa(gatewayPort),
		"-overseer", p.cfg.OverseerAddress,
	}
}

// waitForRegistration polls until the shard registers or the timeout/context is exceeded.
func (p *Provisioner) waitForRegistration(ctx context.Context, addr string) error {
	deadline := time.NewTimer(p.cfg.RegistrationTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(p.cfg.RegistrationPollInterval)
	defer ticker.Stop()

	for {
		if p.cfg.IsShardRegistered(addr) {
			log.Printf("[INFO] provisioner: shard %s registered", addr)
			// Give the shard time to fully start before the observer begins polling.
			if p.cfg.PostRegistrationDelay > 0 {
				if err := p.sleep(ctx, p.cfg.PostRegistrationDelay); err != nil {
					return err
				}
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("shard %s did not register within %v", addr, p.cfg.RegistrationTimeout)
		case <-ticker.C:
			// Poll again on next tick.
		}
	}
}

// sleep waits for d or until ctx is cancelled.
func (p *Provisioner) sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// killProcessGroup sends SIGTERM to the process group, then SIGKILL if it does not exit within 10s or ctx is cancelled.
func (p *Provisioner) killProcessGroup(ctx context.Context, cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	// Use process group so child processes are killed too.
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill() // fallback to single process
		return nil
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	// Wait for graceful exit, with hard limit or ctx cancel.
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
