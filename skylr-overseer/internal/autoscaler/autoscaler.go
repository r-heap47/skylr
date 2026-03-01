package autoscaler

import (
	"context"
	"log"
	"time"

	"github.com/r-heap47/skylr/skylr-overseer/internal/provisioner"
)

// AggregatedMetrics holds metrics aggregated across all shards.
type AggregatedMetrics struct {
	ShardCount    int
	TotalItems    uint64
	AvgCPU        float64
	TotalRSSBytes uint64
	// cumulative op counters (summed across all shards)
	TotalGets    uint64
	TotalSets    uint64
	TotalDeletes uint64
}

// MetricsCollector is a function that returns a fresh AggregatedMetrics snapshot.
// Implemented by the overseer; passed in via Config to avoid a circular import.
type MetricsCollector func() AggregatedMetrics

// Config holds tuning parameters for the autoscaler loop.
type Config struct {
	// EvalInterval is how often metrics are evaluated.
	EvalInterval time.Duration
	// Cooldown is the minimum time between consecutive scale-up operations.
	// Gives migrateKeys time to redistribute keys before re-evaluating.
	Cooldown time.Duration
	// SustainedFor is the number of consecutive evaluation ticks a rule must
	// fire before a scale-up is triggered.
	SustainedFor int
	// Rules is the ordered list of scaling rules to evaluate each tick.
	Rules []ScalingRule
	// CollectMetrics returns the current aggregated metrics snapshot.
	CollectMetrics MetricsCollector
}

// Autoscaler reads cached metrics from the overseer and provisions new shards
// when the configured rules fire for SustainedFor consecutive ticks.
type Autoscaler struct {
	cfg  Config
	prov provisioner.ShardProvisioner
}

// New creates an Autoscaler. Call Run(ctx) to start it.
func New(prov provisioner.ShardProvisioner, cfg Config) *Autoscaler {
	return &Autoscaler{cfg: cfg, prov: prov}
}

// Run starts the autoscaler loop. It blocks until ctx is cancelled.
func (a *Autoscaler) Run(ctx context.Context) {
	if len(a.cfg.Rules) == 0 {
		log.Printf("[INFO] autoscaler: no rules configured, autoscaler is a no-op")
		return
	}

	ticker := time.NewTicker(a.cfg.EvalInterval)
	defer ticker.Stop()

	var (
		consecutiveBreaches int
		lastScaleUp         time.Time
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		agg := a.cfg.CollectMetrics()

		triggered, reason := a.evaluateRules(agg)
		if triggered {
			consecutiveBreaches++
		} else {
			consecutiveBreaches = 0
		}

		if consecutiveBreaches < a.cfg.SustainedFor {
			continue
		}

		// Sustained breach confirmed — check cooldown.
		if elapsed := time.Since(lastScaleUp); elapsed < a.cfg.Cooldown {
			log.Printf("[INFO] autoscaler: scale-up suppressed by cooldown (%.0fs remaining), reason: %s",
				(a.cfg.Cooldown - elapsed).Seconds(), reason)
			continue
		}

		log.Printf("[INFO] autoscaler: triggering scale-up after %d consecutive breaches, reason: %s",
			consecutiveBreaches, reason)

		addr, err := a.prov.Provision(ctx)
		if err != nil {
			log.Printf("[ERROR] autoscaler: provision failed: %s", err)
			// reset breach counter so we don't spam provision attempts on hard errors
			consecutiveBreaches = 0
			continue
		}

		log.Printf("[INFO] autoscaler: provisioned new shard %s", addr)
		consecutiveBreaches = 0
		lastScaleUp = time.Now()
	}
}

// evaluateRules runs all rules and returns (true, reason) for the first one that fires.
func (a *Autoscaler) evaluateRules(agg AggregatedMetrics) (bool, string) {
	for _, rule := range a.cfg.Rules {
		if triggered, reason := rule.Evaluate(agg); triggered {
			return true, reason
		}
	}

	return false, ""
}
