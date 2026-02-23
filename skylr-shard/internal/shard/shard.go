package shard

import (
	"context"
	"fmt"
	"iter"
	"log"
	"time"

	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"github.com/r-heap47/skylr/skylr-shard/internal/pkg/utils"
	"github.com/r-heap47/skylr/skylr-shard/internal/storage"
)

// Shard - basically a shard...
type Shard struct {
	storage storage.Storage
	curTime utils.Provider[time.Time]
}

// Config - shard config
type Config struct {
	Storage storage.Storage
	CurTime utils.Provider[time.Time]
}

// New creates a new shard
func New(cfg Config) *Shard {
	sh := &Shard{
		storage: cfg.Storage,
		curTime: cfg.CurTime,
	}

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

// Delete removes entry from storage by key
// Returns whether the key existed before deletion
func (sh *Shard) Delete(ctx context.Context, k string) (bool, error) {
	return sh.storage.Delete(ctx, k)
}

// ScanEntry is a single entry produced by Scan, carrying the protobuf entry
// and the remaining TTL at the time the scan was taken.
type ScanEntry struct {
	Entry        *pbshard.Entry
	RemainingTTL time.Duration
}

// Scan yields all non-expired entries as protobuf-ready structs.
func (sh *Shard) Scan(ctx context.Context) iter.Seq2[*ScanEntry, error] {
	return func(yield func(*ScanEntry, error) bool) {
		now := sh.curTime(ctx)

		for entry, err := range sh.storage.Scan(ctx) {
			if err != nil {
				yield(nil, err)
				return
			}
			if entry == nil {
				log.Println("[WARN] Shard.Scan: received nil entry")
				continue
			}

			pbEntry := &pbshard.Entry{Key: entry.K}
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
				yield(nil, fmt.Errorf("unsupported value type %T for key %q", v, entry.K))
				return
			}

			remaining := entry.Exp.Sub(now)
			if !yield(&ScanEntry{Entry: pbEntry, RemainingTTL: remaining}, nil) {
				return
			}
		}
	}
}
