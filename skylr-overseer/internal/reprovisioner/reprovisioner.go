package reprovisioner

import (
	"context"
	"log"
	"time"

	"github.com/r-heap47/skylr/skylr-overseer/internal/provisioner"
)

// Config holds tuning parameters for the Reprovisioner.
type Config struct {
	// Failures is the channel on which the overseer publishes failed shard addresses.
	Failures   <-chan string
	MaxRetries int // number of Provision attempts before giving up; 0 = unlimited
	// InitialRetryDelay is the wait before the first retry attempt.
	// Subsequent delays: delay(n) = InitialRetryDelay × 2^(n-1), capped at MaxRetryDelay.
	InitialRetryDelay time.Duration
	// MaxRetryDelay caps the exponential growth of the retry interval.
	MaxRetryDelay time.Duration
}

// Reprovisioner replaces failed shards by deprovisioning the corpse and provisioning a
// fresh replacement with exponential backoff.
type Reprovisioner struct {
	cfg  Config
	prov provisioner.ShardProvisioner
}

// New creates a Reprovisioner. Call Run to start it.
func New(prov provisioner.ShardProvisioner, cfg Config) *Reprovisioner {
	return &Reprovisioner{cfg: cfg, prov: prov}
}

// Run consumes failure notifications and spawns a handler goroutine per failure.
// Blocks until ctx is cancelled.
func (r *Reprovisioner) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case addr := <-r.cfg.Failures:
			go r.handle(ctx, addr)
		}
	}
}

// handle deprovisions the failed shard and provisions a replacement with backoff.
func (r *Reprovisioner) handle(ctx context.Context, addr string) {
	log.Printf("[INFO] reprovisioner: handling failed shard %s", addr)

	if err := r.prov.Deprovision(ctx, addr); err != nil {
		// best-effort: log and continue — replacement must be provisioned regardless
		log.Printf("[WARN] reprovisioner: deprovision %q: %s", addr, err)
	}

	delay := r.cfg.InitialRetryDelay
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return
		}

		newAddr, err := r.prov.Provision(ctx)
		if err == nil {
			log.Printf("[INFO] reprovisioner: replacement for %s provisioned as %s (attempt %d)",
				addr, newAddr, attempt)
			return
		}

		log.Printf("[ERROR] reprovisioner: provision attempt %d for replacement of %s: %s",
			attempt, addr, err)

		if r.cfg.MaxRetries > 0 && attempt >= r.cfg.MaxRetries {
			log.Printf("[ERROR] reprovisioner: giving up after %d attempts for shard %s", attempt, addr)
			return
		}

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}

		// Exponential backoff: delay(n) = InitialRetryDelay × 2^(n-1).
		delay *= 2
		if delay > r.cfg.MaxRetryDelay {
			delay = r.cfg.MaxRetryDelay
		}
	}
}
