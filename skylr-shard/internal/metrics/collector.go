package metrics

import (
	"context"
	"fmt"

	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
	"golang.org/x/sync/errgroup"

	"github.com/shirou/gopsutil/v4/cpu"
)

type Collector struct {
	StorageStr     storage.Storage[string]
	StorageInt64   storage.Storage[int64]
	StorageInt32   storage.Storage[int32]
	StorageFloat64 storage.Storage[float64]
	StorageFloat32 storage.Storage[float32]
}

func (c *Collector) NumElements(ctx context.Context) (int, error) {
	var (
		lenStr     int
		lenInt32   int
		lenInt64   int
		lenFloat32 int
		lenFloat64 int
	)

	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() (err error) {
		lenStr, err = c.StorageStr.Len(egCtx)
		return err
	})

	eg.Go(func() (err error) {
		lenInt32, err = c.StorageInt32.Len(egCtx)
		return err
	})

	eg.Go(func() (err error) {
		lenInt64, err = c.StorageInt64.Len(egCtx)
		return err
	})

	eg.Go(func() (err error) {
		lenFloat32, err = c.StorageFloat32.Len(egCtx)
		return err
	})

	eg.Go(func() (err error) {
		lenFloat64, err = c.StorageFloat64.Len(egCtx)
		return err
	})

	err := eg.Wait()
	if err != nil {
		return 0, err
	}

	return lenStr + lenInt32 + lenInt64 + lenFloat32 + lenFloat64, nil
}

func (c *Collector) UsageCPU(ctx context.Context) (float64, error) {
	usages, err := cpu.PercentWithContext(ctx, 0, false)
	if err != nil {
		return 0, fmt.Errorf("PercentWithContext: %w", err)
	}

	return usages[0], nil
}
