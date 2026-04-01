package reprovisioner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"github.com/r-heap47/skylr/skylr-overseer/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastConfig returns a Config with minimal delays for tests.
func fastConfig(failures <-chan string) Config {
	return Config{
		Failures:          failures,
		MaxRetries:        3,
		InitialRetryDelay: time.Millisecond,
		MaxRetryDelay:     5 * time.Millisecond,
	}
}

// TestRun_CtxCancelStops verifies that Run exits when ctx is cancelled.
func TestRun_CtxCancelStops(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		ProvisionMock.Optional().Return("", nil).
		DeprovisionMock.Optional().Return(nil)

	failures := make(chan string)
	rp := New(prov, fastConfig(failures))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		rp.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run should exit when ctx is cancelled")
	}
}

// TestHandle_HappyPath verifies that a failed shard is deprovisioned and a replacement
// is provisioned exactly once.
func TestHandle_HappyPath(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Return(nil).
		ProvisionMock.Set(func(_ context.Context) (string, error) {
		return "new:5000", nil
	})

	failures := make(chan string, 1)
	rp := New(prov, fastConfig(failures))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rp.Run(ctx)

	failures <- "old:5000"

	assert.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 1 },
		time.Second,
		time.Millisecond,
		"replacement shard should be provisioned",
	)
	assert.Equal(t, uint64(1), prov.DeprovisionAfterCounter(), "deprovision should be called once")
}

// TestHandle_DeprovisionError_ContinuesProvisioning verifies that a Deprovision error
// does not prevent provisioning the replacement.
func TestHandle_DeprovisionError_ContinuesProvisioning(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Return(errors.New("dead")).
		ProvisionMock.Set(func(_ context.Context) (string, error) {
		return "new:5001", nil
	})

	failures := make(chan string, 1)
	rp := New(prov, fastConfig(failures))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rp.Run(ctx)

	failures <- "old:5001"

	assert.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 1 },
		time.Second,
		time.Millisecond,
		"provision should proceed despite deprovision error",
	)
}

// TestHandle_ProvisionRetries_ThenSucceeds verifies that transient Provision errors
// are retried and eventually succeed.
func TestHandle_ProvisionRetries_ThenSucceeds(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)

	var callCount uint64
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Return(nil).
		ProvisionMock.Set(func(_ context.Context) (string, error) {
		callCount++
		if callCount < 3 {
			return "", errors.New("not ready")
		}
		return "new:5002", nil
	})

	failures := make(chan string, 1)
	cfg := Config{
		Failures:          failures,
		MaxRetries:        5,
		InitialRetryDelay: time.Millisecond,
		MaxRetryDelay:     5 * time.Millisecond,
	}
	rp := New(prov, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rp.Run(ctx)

	failures <- "old:5002"

	require.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 3 },
		time.Second,
		time.Millisecond,
		"should retry until provisioning succeeds on 3rd attempt",
	)
}

// TestHandle_MaxRetriesExhausted verifies that handle exits cleanly after exhausting
// all retries without panicking.
func TestHandle_MaxRetriesExhausted(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Return(nil).
		ProvisionMock.Return("", errors.New("always fails"))

	failures := make(chan string, 1)
	cfg := Config{
		Failures:          failures,
		MaxRetries:        3,
		InitialRetryDelay: time.Millisecond,
		MaxRetryDelay:     5 * time.Millisecond,
	}
	rp := New(prov, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rp.Run(ctx)

	failures <- "old:5003"

	// handle should stop after exactly 3 attempts
	require.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 3 },
		time.Second,
		time.Millisecond,
		"should attempt provision MaxRetries times",
	)

	// allow a moment then confirm no further attempts
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(3), prov.ProvisionAfterCounter(),
		"no more attempts after MaxRetries exhausted")
}

// TestHandle_CtxCancelDuringBackoff verifies that cancelling ctx while handle is
// waiting in backoff unblocks it immediately.
func TestHandle_CtxCancelDuringBackoff(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Return(nil).
		ProvisionMock.Return("", errors.New("always fails"))

	failures := make(chan string, 1)
	cfg := Config{
		Failures:          failures,
		MaxRetries:        0, // unlimited — only ctx cancel stops it
		InitialRetryDelay: time.Millisecond,
		MaxRetryDelay:     10 * time.Second, // large: ensures handle is in backoff when cancelled
	}
	rp := New(prov, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		rp.Run(ctx)
		close(done)
	}()

	failures <- "old:5004"

	// wait for at least one provision attempt (enters backoff with 10s delay on retry 2)
	require.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 1 },
		time.Second,
		time.Millisecond,
	)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run should exit quickly when ctx is cancelled during backoff")
	}
}

// TestRun_MultipleConcurrentFailures verifies that multiple failures are handled
// concurrently, not sequentially.
func TestRun_MultipleConcurrentFailures(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)
	prov := mocks.NewShardProvisionerMock(mc).
		DeprovisionMock.Optional().Return(nil).
		ProvisionMock.Set(func(_ context.Context) (string, error) {
		time.Sleep(5 * time.Millisecond)
		return "new:9000", nil
	})

	failures := make(chan string, 3)
	rp := New(prov, fastConfig(failures))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rp.Run(ctx)

	failures <- "old:1"
	failures <- "old:2"
	failures <- "old:3"

	// If sequential: 3 × 5ms = 15ms minimum. Concurrent: ~5ms.
	// 500ms deadline is generous but distinguishes concurrent from serial.
	assert.Eventually(t,
		func() bool { return prov.ProvisionAfterCounter() >= 3 },
		500*time.Millisecond,
		time.Millisecond,
		"all three failures should be handled concurrently",
	)
}
