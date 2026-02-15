package metrics

import (
	"testing"
)

func TestOpsCounters(t *testing.T) {
	t.Parallel()

	// Reset counters to known state
	getOps.Store(0)
	setOps.Store(0)
	deleteOps.Store(0)

	// Test initial state
	if got := TotalGets(); got != 0 {
		t.Errorf("TotalGets() = %d, want 0", got)
	}
	if got := TotalSets(); got != 0 {
		t.Errorf("TotalSets() = %d, want 0", got)
	}
	if got := TotalDeletes(); got != 0 {
		t.Errorf("TotalDeletes() = %d, want 0", got)
	}

	// Test increments
	IncGetOps()
	if got := TotalGets(); got != 1 {
		t.Errorf("After IncGetOps(), TotalGets() = %d, want 1", got)
	}

	IncSetOps()
	IncSetOps()
	if got := TotalSets(); got != 2 {
		t.Errorf("After 2x IncSetOps(), TotalSets() = %d, want 2", got)
	}

	IncDeleteOps()
	IncDeleteOps()
	IncDeleteOps()
	if got := TotalDeletes(); got != 3 {
		t.Errorf("After 3x IncDeleteOps(), TotalDeletes() = %d, want 3", got)
	}

	// Test concurrent increments
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func() {
			IncGetOps()
			done <- true
		}()
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	if got := TotalGets(); got != 101 {
		t.Errorf("After 100 concurrent IncGetOps(), TotalGets() = %d, want 101", got)
	}
}
