package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
)

// bench is the common interface for all storage systems under test.
type bench interface {
	set(ctx context.Context, key, value string) error
	get(ctx context.Context, key string) error
	close() error
}

// --- Redis/Valkey/KeyDB target (RESP protocol, identical client) ---

type redisBench struct{ c *redis.Client }

func newRedisBench(addr string) *redisBench {
	return &redisBench{c: redis.NewClient(&redis.Options{
		Addr:     addr,
		PoolSize: 128,
	})}
}

func (b *redisBench) set(ctx context.Context, key, value string) error {
	return b.c.Set(ctx, key, value, 0).Err()
}

func (b *redisBench) get(ctx context.Context, key string) error {
	_, err := b.c.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}
	return err
}

func (b *redisBench) close() error { return b.c.Close() }

// --- Memcached target ---

type memcachedBench struct{ c *memcache.Client }

func newMemcachedBench(addr string) *memcachedBench {
	mc := memcache.New(addr)
	mc.MaxIdleConns = 128
	return &memcachedBench{c: mc}
}

func (b *memcachedBench) set(_ context.Context, key, value string) error {
	return b.c.Set(&memcache.Item{Key: key, Value: []byte(value)})
}

func (b *memcachedBench) get(_ context.Context, key string) error {
	_, err := b.c.Get(key)
	if err == memcache.ErrCacheMiss {
		return nil
	}
	return err
}

func (b *memcachedBench) close() error { return nil }

// --- Benchmark runner ---

type runResult struct {
	setLats []int64 // nanoseconds, per-operation
	getLats []int64
}

// warmup sequentially writes all keys so subsequent GETs hit warm data.
func warmup(ctx context.Context, b bench, keySpace int, val string) {
	fmt.Printf("  warmup (%d keys)... ", keySpace)
	for i := range keySpace {
		_ = b.set(ctx, fmt.Sprintf("k%d", i), val)
	}
	fmt.Println("done")
}

// runBench launches concurrency workers that perform mixed GET/SET operations
// for the given duration. Each worker records its own latency slice to avoid
// any shared-memory synchronisation on the hot path.
func runBench(ctx context.Context, b bench, concurrency int, dur time.Duration, keySpace int, val string, rwRatio float64) runResult {
	setSlices := make([][]int64, concurrency)
	getSlices := make([][]int64, concurrency)
	for i := range concurrency {
		setSlices[i] = make([]int64, 0, 50_000)
		getSlices[i] = make([]int64, 0, 50_000)
	}

	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup

	for i := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(id), 0))
			sl := setSlices[id]
			gl := getSlices[id]

			for time.Now().Before(deadline) {
				key := fmt.Sprintf("k%d", rng.IntN(keySpace))
				if rng.Float64() < rwRatio {
					start := time.Now()
					_ = b.get(ctx, key)
					gl = append(gl, time.Since(start).Nanoseconds())
				} else {
					start := time.Now()
					_ = b.set(ctx, key, val)
					sl = append(sl, time.Since(start).Nanoseconds())
				}
			}

			setSlices[id] = sl
			getSlices[id] = gl
		}(i)
	}

	wg.Wait()

	// Merge per-worker slices into one for reporting.
	var r runResult
	for i := range concurrency {
		r.setLats = append(r.setLats, setSlices[i]...)
		r.getLats = append(r.getLats, getSlices[i]...)
	}
	return r
}

// pct returns the value at percentile p (0.0–1.0) of a sorted slice.
func pct(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func printRow(name, op string, lats []int64, dur time.Duration) {
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	opsPerSec := float64(len(lats)) / dur.Seconds()
	p50 := pct(lats, 0.50) / 1_000 // ns → µs
	p99 := pct(lats, 0.99) / 1_000
	fmt.Printf("%-12s  %-3s  %10.0f  %8d  %8d\n", name, op, opsPerSec, p50, p99)
}

func main() {
	target := flag.String("target", "all", "system: redis|valkey|keydb|memcached|all")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	valkeyAddr := flag.String("valkey", "localhost:6380", "Valkey address")
	keydbAddr := flag.String("keydb", "localhost:6381", "KeyDB address")
	memcAddr := flag.String("memcached", "localhost:11212", "Memcached address")
	concurrency := flag.Int("c", 50, "concurrent workers")
	dur := flag.Duration("d", 30*time.Second, "benchmark duration per system")
	keySpace := flag.Int("keys", 100_000, "key space size")
	valueSize := flag.Int("vsize", 256, "value size in bytes")
	rwRatio := flag.Float64("rw", 0.8, "fraction of operations that are GETs")
	flag.Parse()

	ctx := context.Background()
	val := strings.Repeat("x", *valueSize)

	type entry struct {
		name  string
		addr  string
		newFn func(string) bench
	}

	all := []entry{
		{"redis", *redisAddr, func(a string) bench { return newRedisBench(a) }},
		{"valkey", *valkeyAddr, func(a string) bench { return newRedisBench(a) }},
		{"keydb", *keydbAddr, func(a string) bench { return newRedisBench(a) }},
		{"memcached", *memcAddr, func(a string) bench { return newMemcachedBench(a) }},
	}

	fmt.Printf("%-12s  %-3s  %10s  %8s  %8s\n", "system", "op", "ops/sec", "p50µs", "p99µs")
	fmt.Println(strings.Repeat("─", 50))

	for _, e := range all {
		if *target != "all" && *target != e.name {
			continue
		}
		fmt.Printf("[%s @ %s]\n", e.name, e.addr)
		b := e.newFn(e.addr)
		warmup(ctx, b, *keySpace, val)
		r := runBench(ctx, b, *concurrency, *dur, *keySpace, val, *rwRatio)
		printRow(e.name, "SET", r.setLats, *dur)
		printRow(e.name, "GET", r.getLats, *dur)
		_ = b.close()
	}
}
