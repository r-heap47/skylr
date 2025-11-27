package shard

import (
	"context"
	stderrs "errors"
	"fmt"

	"github.com/cutlery47/skylr/skylr-shard/internal/pkg/errors"
	"github.com/cutlery47/skylr/skylr-shard/internal/storage"
	pbshard "github.com/cutlery47/skylr/skylr-shard/pkg/pb/skylr-shard"

	"golang.org/x/sync/errgroup"
)

// Shard - basically a shard...
type Shard struct {
	storageStr     storage.Storage[string]
	storageInt64   storage.Storage[int64]
	storageInt32   storage.Storage[int32]
	storageFloat64 storage.Storage[float64]
	storageFloat32 storage.Storage[float32]
}

// Get searches for entry in each storage by provided key
func (sh *Shard) Get(ctx context.Context, k string) (*pbshard.Entry, error) {
	var (
		entry = pbshard.Entry{
			Key: k,
		}

		entryStr     *storage.Entry[string]
		entryInt64   *storage.Entry[int64]
		entryInt32   *storage.Entry[int32]
		entryFloat64 *storage.Entry[float64]
		entryFloat32 *storage.Entry[float32]

		eg, egCtx = errgroup.WithContext(ctx)
	)

	// concurrently check for the entry in each typed storage
	// if entry was not found in one of the storages - continue searching

	eg.Go(func() error {
		gotStr, err := sh.storageStr.Get(egCtx, k)
		if err != nil && !stderrs.Is(err, errors.ErrNotFound) {
			return fmt.Errorf("storageString.Get: %w", err)
		}
		entryStr = gotStr

		return nil
	})

	eg.Go(func() error {
		gotInt64, err := sh.storageInt64.Get(egCtx, k)
		if err != nil && !stderrs.Is(err, errors.ErrNotFound) {
			return fmt.Errorf("storageInt64.Get: %w", err)
		}
		entryInt64 = gotInt64

		return nil
	})

	eg.Go(func() error {
		gotInt32, err := sh.storageInt32.Get(egCtx, k)
		if err != nil && !stderrs.Is(err, errors.ErrNotFound) {
			return fmt.Errorf("storageInt32.Get: %w", err)
		}
		entryInt32 = gotInt32

		return nil
	})

	eg.Go(func() error {
		gotFloat64, err := sh.storageFloat64.Get(egCtx, k)
		if err != nil && !stderrs.Is(err, errors.ErrNotFound) {
			return fmt.Errorf("storageFloat64.Get: %w", err)
		}
		entryFloat64 = gotFloat64

		return nil
	})

	eg.Go(func() error {
		gotFloat32, err := sh.storageFloat32.Get(egCtx, k)
		if err != nil && !stderrs.Is(err, errors.ErrNotFound) {
			return fmt.Errorf("storageFloat32.Get: %w", err)
		}
		entryFloat32 = gotFloat32

		return nil
	})

	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	switch {
	case entryStr != nil:
		entry.Value = &pbshard.Entry_ValueStr{
			ValueStr: entryStr.V,
		}
	case entryInt64 != nil:
		entry.Value = &pbshard.Entry_ValueInt64{
			ValueInt64: entryInt64.V,
		}
	case entryInt32 != nil:
		entry.Value = &pbshard.Entry_ValueInt32{
			ValueInt32: entryInt32.V,
		}
	case entryFloat64 != nil:
		entry.Value = &pbshard.Entry_ValueDouble{
			ValueDouble: entryFloat64.V,
		}
	case entryFloat32 != nil:
		entry.Value = &pbshard.Entry_ValueFloat{
			ValueFloat: entryFloat32.V,
		}
	default:
		return nil, errors.ErrNotFound
	}

	return &entry, nil
}
