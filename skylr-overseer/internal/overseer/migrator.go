package overseer

import (
	"context"
	"io"
	"log"

	"github.com/r-heap47/skylr/skylr-overseer/internal/hashring"
	pbshard "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-shard"
	"google.golang.org/protobuf/types/known/emptypb"
)

// migrateKeys moves keys that now belong to newAddr (according to the current ring)
// but previously lived on other shards (according to oldRing).
//
// It runs in a background goroutine; errors are logged but never fatal.
// If the context is cancelled (overseer shutdown) the migration stops early.
func (ovr *Overseer) migrateKeys(ctx context.Context, snap *hashring.HashRing, newAddr string) {
	if snap.Size() == 0 {
		return
	}

	// finding source shards and the destination shard
	srcs, dst := func() ([]shard, *shard) {
		ovr.shardsMu.RLock()
		defer ovr.shardsMu.RUnlock()

		srcs := make([]shard, 0, len(ovr.shards))

		dst, hasDst := ovr.shards[newAddr]
		if !hasDst {
			log.Printf("[WARN] migration: new shard %s not found in registry, skipping", newAddr)
			return nil, nil
		}

		for addr, s := range ovr.shards {
			if addr != newAddr {
				srcs = append(srcs, s)
			}
		}

		log.Printf("[INFO] migration: scanning %d source shard(s) for keys to move to %s", len(srcs), newAddr)
		return srcs, &dst
	}()
	if dst == nil {
		return
	}

	moved := 0
	for _, src := range srcs {
		n, err := ovr.migrateFromShard(ctx, src, *dst, snap, newAddr)
		if err != nil {
			log.Printf("[WARN] migration: scan of shard %s failed after %d moves: %s", src.addr, n, err)
		}

		moved += n
	}

	log.Printf("[INFO] migration: completed, moved %d key(s) to %s", moved, newAddr)
}

// migrateFromShard scans src and moves any key that now belongs to newAddr.
// Phase 1: collect entries during Scan (no Delete on src — avoids deadlock where
// Scan holds RLock and Delete needs Lock). Phase 2: after Scan completes, move
// each collected entry (Set on dst, Delete on src).
func (ovr *Overseer) migrateFromShard(
	ctx context.Context,
	src shard,
	dst shard,
	snap *hashring.HashRing,
	newAddr string,
) (int, error) {
	stream, err := src.client.Scan(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, err
	}

	// Phase 1: collect entries that belong to newAddr; do NOT call moveKey yet
	// (moveKey does src.Delete, which would deadlock while Scan holds RLock).
	var toMove []*pbshard.ScanResponse
	for {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		resp, err := stream.Recv()
		if err != nil {
			if isStreamEOF(err) {
				break
			}
			return 0, err
		}

		key := resp.Entry.Key
		if ovr.keyMovedToNewShard(key, snap, newAddr) {
			toMove = append(toMove, resp)
		}
	}

	// Phase 2: Scan completed, RLock released; now move each entry.
	moved := 0
	for _, resp := range toMove {
		if ctx.Err() != nil {
			return moved, ctx.Err()
		}

		key := resp.Entry.Key
		ok, err := ovr.moveKey(ctx, src, dst, resp)
		if err != nil {
			log.Printf("[WARN] migration: failed to move key %q from %s to %s: %s", key, src.addr, dst.addr, err)
			continue
		}
		if ok {
			moved++
		}
	}

	return moved, nil
}

// keyMovedToNewShard reports whether key has been reassigned to newAddr by the
// ring update: it must belong to newAddr now but have belonged to someone else before.
func (ovr *Overseer) keyMovedToNewShard(key string, snap *hashring.HashRing, newAddr string) bool {
	oldOwner, err := snap.GetNode(key)
	if err != nil {
		return false
	}
	newOwner, err := ovr.ring.GetNode(key)
	if err != nil {
		return false
	}

	return newOwner == newAddr && oldOwner != newAddr
}

// moveKey sets the entry on dst and deletes it from src.
// Returns false without doing anything if the entry has already expired.
func (ovr *Overseer) moveKey(ctx context.Context, src shard, dst shard, resp *pbshard.ScanResponse) (bool, error) {
	if resp.RemainingTtl == nil || resp.RemainingTtl.AsDuration() <= 0 {
		return false, nil
	}

	_, err := dst.client.Set(ctx, &pbshard.SetRequest{
		Input: &pbshard.InputEntry{
			Entry: resp.Entry,
			Ttl:   resp.RemainingTtl,
		},
	})
	if err != nil {
		return false, err
	}

	_, err = src.client.Delete(ctx, &pbshard.DeleteRequest{Key: resp.Entry.Key})
	if err != nil {
		return false, err
	}

	return true, nil
}

// isStreamEOF returns true when err signals that the server-side stream has ended normally.
func isStreamEOF(err error) bool {
	return err == io.EOF
}
