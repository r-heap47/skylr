package metrics

import (
	"context"
	"fmt"

	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
	"github.com/shirou/gopsutil/v4/cpu"
)

// Collector - metric collector
type Collector struct {
	Storage storage.Storage
}

// NumElements calculates the total amount of elements in all storages
func (c *Collector) NumElements(ctx context.Context) (int, error) {
	return c.Storage.Len(ctx)
}

// UsageCPU calculates CPU usage
func (c *Collector) UsageCPU(ctx context.Context) (float64, error) {
	usages, err := cpu.PercentWithContext(ctx, 0, false)
	if err != nil {
		return 0, fmt.Errorf("PercentWithContext: %w", err)
	}

	return usages[0], nil
}
