package autoscaler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"github.com/r-heap47/skylr/skylr-overseer/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staticMetrics returns a MetricsCollector that always yields the given snapshot.
func staticMetrics(agg AggregatedMetrics) MetricsCollector {
	return func() AggregatedMetrics { return agg }
}

// fastConfig returns a Config with near-zero delays for testing.
func fastConfig(collector MetricsCollector, rules []ScalingRule, sustainedFor int, prov *mocks.ShardProvisionerMock) Config {
	return Config{
		EvalInterval:   time.Millisecond,
		Cooldown:       time.Millisecond,
		SustainedFor:   sustainedFor,
		Rules:          rules,
		CollectMetrics: collector,
	}
}

// TestAutoscaler_NoRules verifies that an autoscaler with no rules exits immediately.
func TestAutoscaler_NoRules(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Optional().Return(nil)

	as := New(prov, fastConfig(staticMetrics(AggregatedMetrics{}), nil, 1, prov))

	done := make(chan struct{})
	go func() {
		as.Run(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("autoscaler with no rules should return immediately")
	}
}

// TestAutoscaler_SustainedBreach verifies that Provision is called only after
// the rule fires for SustainedFor consecutive ticks.
func TestAutoscaler_SustainedBreach(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		ProvisionMock.Set(func(_ context.Context) (string, error) {
		return "localhost:5011", nil
	}).
		DeprovisionMock.Optional().Return(nil)

	const sustainedFor = 3
	rule := ItemCountRule{Threshold: 1} // always triggers (ShardCount=1, TotalItems=100)
	agg := AggregatedMetrics{ShardCount: 1, TotalItems: 100}

	as := New(prov, fastConfig(staticMetrics(agg), []ScalingRule{rule}, sustainedFor, prov))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go as.Run(ctx)

	assert.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 1 },
		time.Second,
		time.Millisecond,
		"should provision after sustained breach",
	)
}

// TestAutoscaler_NoTriggerBelowSustained verifies that if the rule fires fewer
// than SustainedFor consecutive ticks, no provision is made.
func TestAutoscaler_NoTriggerBelowSustained(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Optional().Return(nil).
		ProvisionMock.Optional().Return("localhost:5012", nil)

	const sustainedFor = 5

	// Rule alternates: fires on odd calls, does not fire on even calls —
	// so it never accumulates sustainedFor consecutive breaches.
	var callCount atomic.Int64
	alternatingRule := &alternatingScalingRule{calls: &callCount}

	cfg := Config{
		EvalInterval:   time.Millisecond,
		Cooldown:       time.Millisecond,
		SustainedFor:   sustainedFor,
		Rules:          []ScalingRule{alternatingRule},
		CollectMetrics: staticMetrics(AggregatedMetrics{}),
	}
	as := New(prov, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	go as.Run(ctx)

	// let it run through several cycles
	time.Sleep(30 * time.Millisecond)
	cancel()

	assert.Equal(t, uint64(0), prov.ProvisionAfterCounter(),
		"alternating rule should never accumulate enough consecutive breaches")
}

// TestAutoscaler_CooldownSuppressesRepeat verifies that after one scale-up,
// a second scale-up is suppressed until the cooldown has elapsed.
func TestAutoscaler_CooldownSuppressesRepeat(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		ProvisionMock.Set(func(_ context.Context) (string, error) {
		return "localhost:5013", nil
	}).
		DeprovisionMock.Optional().Return(nil)

	rule := ItemCountRule{Threshold: 1}
	agg := AggregatedMetrics{ShardCount: 1, TotalItems: 100}

	cfg := Config{
		EvalInterval:   time.Millisecond,
		Cooldown:       50 * time.Millisecond, // noticeable cooldown
		SustainedFor:   1,
		Rules:          []ScalingRule{rule},
		CollectMetrics: staticMetrics(agg),
	}
	as := New(prov, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go as.Run(ctx)

	// wait for the first provision
	require.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 1 },
		time.Second,
		time.Millisecond,
	)

	firstCount := prov.ProvisionAfterCounter()

	// within the cooldown window no extra provisions should fire
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, firstCount, prov.ProvisionAfterCounter(),
		"second provision should be suppressed during cooldown")
}

// alternatingScalingRule fires on odd-numbered calls and skips even ones.
type alternatingScalingRule struct {
	calls *atomic.Int64
}

func (r *alternatingScalingRule) Evaluate(_ AggregatedMetrics) (bool, string) {
	n := r.calls.Add(1)
	if n%2 == 1 {
		return true, "odd call"
	}
	return false, ""
}
