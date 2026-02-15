package shard

import (
	"context"
	"fmt"
	"log"
	"time"

	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
)

// Shard - basically a shard...
type Shard struct {
	storage storage.Storage

	curTime         utils.Provider[time.Time]
	cleanupTimeout  utils.Provider[time.Duration]
	cleanupCooldown utils.Provider[time.Duration]

	start <-chan struct{}
}

// Config - shard config
type Config struct {
	Storage storage.Storage

	CurTime         utils.Provider[time.Time]
	CleanupTimeout  utils.Provider[time.Duration] // cooldown between cleanups
	CleanupCooldown utils.Provider[time.Duration]

	Start <-chan struct{}
}

// New creates a new shard
func New(cfg Config) *Shard {
	sh := &Shard{
		storage:         cfg.Storage,
		curTime:         cfg.CurTime,
		cleanupCooldown: cfg.CleanupCooldown,
		cleanupTimeout:  cfg.CleanupTimeout,
		start:           cfg.Start,
	}

	// init cleanup process
	defer func() {
		go sh.cleanup()
	}()

	return sh
}

// Get searches for entry in each storage by provided key
func (sh *Shard) Get(ctx context.Context, k string) (*pbshard.Entry, error) {
	entry, err := sh.storage.Get(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("storage.Get: %w", err)
	}

	pbEntry := &pbshard.Entry{Key: entry.K}

	// Type assertion для конвертации any → protobuf oneof
	switch v := entry.V.(type) {
	case string:
		pbEntry.Value = &pbshard.Entry_ValueStr{ValueStr: v}
	case int64:
		pbEntry.Value = &pbshard.Entry_ValueInt64{ValueInt64: v}
	case int32:
		pbEntry.Value = &pbshard.Entry_ValueInt32{ValueInt32: v}
	case float64:
		pbEntry.Value = &pbshard.Entry_ValueDouble{ValueDouble: v}
	case float32:
		pbEntry.Value = &pbshard.Entry_ValueFloat{ValueFloat: v}
	default:
		return nil, fmt.Errorf("unsupported value type: %T", v)
	}

	return pbEntry, nil
}

// Set uploads new entry to storage
func (sh *Shard) Set(ctx context.Context, in *pbshard.InputEntry) error {
	exp := sh.curTime(ctx).Add(in.Ttl.AsDuration())
	entry := storage.Entry{
		K:   in.Entry.Key,
		Exp: exp,
	}

	// Извлекаем конкретное значение из protobuf oneof
	switch val := in.Entry.Value.(type) {
	case *pbshard.Entry_ValueStr:
		entry.V = val.ValueStr
	case *pbshard.Entry_ValueInt64:
		entry.V = val.ValueInt64
	case *pbshard.Entry_ValueInt32:
		entry.V = val.ValueInt32
	case *pbshard.Entry_ValueDouble:
		entry.V = val.ValueDouble
	case *pbshard.Entry_ValueFloat:
		entry.V = val.ValueFloat
	default:
		return fmt.Errorf("unsupported value type")
	}

	_, err := sh.storage.Set(ctx, entry)
	return err
}

// cleanup performs cleanup-loop
func (sh *Shard) cleanup() {
	// wait for shard initialization
	<-sh.start

	for {
		ctx := context.Background()

		err := sh.clean(ctx)
		if err != nil {
			// TODO: proper logging
			log.Printf("error when doing cleanup: %s\n", err)
		}

		time.Sleep(sh.cleanupCooldown(ctx))
	}
}

// clean cleans each storage
func (sh *Shard) clean(ctx context.Context) error {
	timeout := sh.cleanupTimeout(ctx)
	now := sh.curTime(ctx)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		errCh <- sh.storage.Clean(ctx, now)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
