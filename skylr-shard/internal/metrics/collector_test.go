package metrics

import (
	"context"
	"testing"
	"time"
)

func TestNewCollector(t *testing.T) {
	t.Parallel()

	startTime := time.Now()
	c := NewCollector(startTime)

	if c == nil {
		t.Fatal("NewCollector() returned nil")
	}

	if c.startTime != startTime {
		t.Errorf("NewCollector() startTime = %v, want %v", c.startTime, startTime)
	}
}

func TestCollectorUptime(t *testing.T) {
	t.Parallel()

	startTime := time.Now().Add(-5 * time.Second)
	c := NewCollector(startTime)

	uptime := c.Uptime()

	if uptime < 5 || uptime > 6 {
		t.Errorf("Uptime() = %d, want ~5 seconds", uptime)
	}
}

func TestCollectorUsageCPU(t *testing.T) {
	t.Parallel()

	c := NewCollector(time.Now())
	ctx := context.Background()

	cpu, err := c.UsageCPU(ctx)
	if err != nil {
		t.Fatalf("UsageCPU() error = %v", err)
	}

	if cpu < 0 || cpu > 100 {
		t.Errorf("UsageCPU() = %f, want value between 0 and 100", cpu)
	}
}

func TestCollectorMemoryRSS(t *testing.T) {
	t.Parallel()

	c := NewCollector(time.Now())
	ctx := context.Background()

	rss, err := c.MemoryRSS(ctx)
	if err != nil {
		t.Fatalf("MemoryRSS() error = %v", err)
	}

	if rss == 0 {
		t.Error("MemoryRSS() = 0, expected non-zero value")
	}
}

func TestCollectorMemoryHeapAlloc(t *testing.T) {
	t.Parallel()

	c := NewCollector(time.Now())
	ctx := context.Background()

	heap, err := c.MemoryHeapAlloc(ctx)
	if err != nil {
		t.Fatalf("MemoryHeapAlloc() error = %v", err)
	}

	if heap == 0 {
		t.Error("MemoryHeapAlloc() = 0, expected non-zero value")
	}
}
