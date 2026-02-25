package overseer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	empty "github.com/golang/protobuf/ptypes/empty"
	pbshard "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-overseer/mocks"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

// fastObserver builds an observer with near-zero delays for use in tests.
func fastObserver(t *testing.T, client pbshard.ShardClient, errChan chan error, threshold int) *observer {
	t.Helper()

	return &observer{
		addr:                "test-shard:9000",
		shardClient:         client,
		errChan:             errChan,
		delay:               utils.Const(time.Millisecond),
		metricsTimeout:      utils.Const(100 * time.Millisecond),
		errorThreshold:      utils.Const(threshold),
		logStorageOnMetrics: utils.Const(false),
	}
}

// metricsOK is a Metrics implementation that always returns empty metrics.
func metricsOK(_ context.Context, _ *empty.Empty, _ ...grpc.CallOption) (*pbshard.MetricsResponse, error) {
	return &pbshard.MetricsResponse{}, nil
}

// metricsErr is a Metrics implementation that always returns an error.
func metricsErr(_ context.Context, _ *empty.Empty, _ ...grpc.CallOption) (*pbshard.MetricsResponse, error) {
	return nil, errors.New("shard unavailable")
}

// waitDone blocks until done is closed or the deadline is exceeded.
func waitDone(t *testing.T, done <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

// TestObserve_CtxCancelStopsLoop verifies that observe returns promptly when
// its context is cancelled without signalling errChan.
func TestObserve_CtxCancelStopsLoop(t *testing.T) {
	t.Parallel()

	mc := minimock.NewController(t)

	errChan := make(chan error, 1)
	shardMock := mocks.NewShardClientMock(mc).
		MetricsMock.Set(metricsOK)

	obs := fastObserver(t, shardMock, errChan, 3)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		obs.observe(ctx)
		close(done)
	}()

	// let the observer run for a bit then cancel
	time.Sleep(10 * time.Millisecond)
	cancel()

	waitDone(t, done, time.Second, "observe did not stop after ctx cancel")
	assert.Empty(t, errChan, "errChan should be empty after ctx cancel")
}

// TestObserve_BelowThresholdNoSignal verifies that N-1 consecutive errors
// followed by a success do NOT signal errChan.
func TestObserve_BelowThresholdNoSignal(t *testing.T) {
	t.Parallel()

	const threshold = 3

	mc := minimock.NewController(t)

	errChan := make(chan error, 1)
	callCount := 0

	shardMock := mocks.NewShardClientMock(mc).
		MetricsMock.Set(func(_ context.Context, _ *empty.Empty, _ ...grpc.CallOption) (*pbshard.MetricsResponse, error) {
		callCount++
		// first threshold-1 calls return error, then success
		if callCount < threshold {
			return nil, errors.New("transient")
		}
		return &pbshard.MetricsResponse{}, nil
	})

	obs := fastObserver(t, shardMock, errChan, threshold)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		obs.observe(ctx)
		close(done)
	}()

	// wait until at least threshold calls have been processed
	time.Sleep(50 * time.Millisecond)
	cancel()

	<-done

	assert.Empty(t, errChan, "N-1 errors followed by success must not signal errChan")
}

// TestObserve_ThresholdReachedSignalsErrChan verifies that N consecutive
// errors cause observe to send to errChan and stop.
func TestObserve_ThresholdReachedSignalsErrChan(t *testing.T) {
	t.Parallel()

	const threshold = 3

	mc := minimock.NewController(t)

	errChan := make(chan error, 1)

	shardMock := mocks.NewShardClientMock(mc).
		MetricsMock.Set(metricsErr)

	obs := fastObserver(t, shardMock, errChan, threshold)

	done := make(chan struct{})
	go func() {
		obs.observe(context.Background())
		close(done)
	}()

	waitDone(t, done, time.Second, "observe did not stop after reaching error threshold")
	assert.Len(t, errChan, 1, "errChan should contain exactly 1 error after threshold is reached")
}

// TestObserve_ErrorStreakResetsOnSuccess verifies that after N-1 errors and
// a success, another N-1 errors still do NOT trigger errChan.
func TestObserve_ErrorStreakResetsOnSuccess(t *testing.T) {
	t.Parallel()

	const threshold = 3

	mc := minimock.NewController(t)

	errChan := make(chan error, 1)
	callCount := 0

	// pattern: err, err, ok, err, err, ok, ... — every 3rd call succeeds
	shardMock := mocks.NewShardClientMock(mc).
		MetricsMock.Set(func(_ context.Context, _ *empty.Empty, _ ...grpc.CallOption) (*pbshard.MetricsResponse, error) {
		callCount++
		if callCount%threshold == 0 {
			return &pbshard.MetricsResponse{}, nil
		}
		return nil, errors.New("transient")
	})

	obs := fastObserver(t, shardMock, errChan, threshold)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		obs.observe(ctx)
		close(done)
	}()

	// run through two full error+success cycles
	time.Sleep(50 * time.Millisecond)
	cancel()

	<-done

	assert.Empty(t, errChan, "success calls must reset the error streak")
}
