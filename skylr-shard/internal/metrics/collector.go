package metrics

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
)

// Collector - system metrics collector
type Collector struct {
	startTime time.Time
}

// NewCollector creates a new Collector
func NewCollector(startTime time.Time) *Collector {
	return &Collector{startTime: startTime}
}

// UsageCPU calculates CPU usage
func (c *Collector) UsageCPU(ctx context.Context) (float64, error) {
	usages, err := cpu.PercentWithContext(ctx, 0, false)
	if err != nil {
		return 0, fmt.Errorf("PercentWithContext: %w", err)
	}

	return usages[0], nil
}

// MemoryRSS returns process RSS (Resident Set Size) memory in bytes
func (c *Collector) MemoryRSS(ctx context.Context) (uint64, error) {
	p, err := process.NewProcessWithContext(ctx, int32(os.Getpid())) // nolint: gosec
	if err != nil {
		return 0, fmt.Errorf("process.NewProcessWithContext: %w", err)
	}

	memInfo, err := p.MemoryInfoWithContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("p.MemoryInfoWithContext: %w", err)
	}

	return memInfo.RSS, nil
}

// MemoryHeapAlloc returns Go heap allocated bytes (live objects only)
func (c *Collector) MemoryHeapAlloc(_ context.Context) (uint64, error) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc, nil
}

// Uptime returns shard uptime in seconds
func (c *Collector) Uptime() int64 {
	return int64(time.Since(c.startTime).Seconds())
}
