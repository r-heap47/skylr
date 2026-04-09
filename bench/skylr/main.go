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

	client "github.com/r-heap47/skylr/skylr-client"
)

// runResult holds per-operation latency samples collected during the benchmark.
type runResult struct {
	setLats []int64 // nanoseconds
	getLats []int64
}

// warmup sequentially writes all keys so subsequent GETs hit warm data.
func warmup(ctx context.Context, c *client.Client, keySpace int, val string) {
	fmt.Printf("  warmup (%d keys)... ", keySpace)
	for i := range keySpace {
		_ = c.Set(ctx, fmt.Sprintf("k%d", i), val, time.Hour)
	}
	fmt.Println("done")
}

// runBench launches concurrency workers that perform mixed GET/SET operations
// for the given duration. Each worker records its own latency slice to avoid
// any shared-memory synchronisation on the hot path.
func runBench(ctx context.Context, c *client.Client, concurrency int, dur time.Duration, keySpace int, val string, rwRatio float64) runResult {
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
					_, _ = c.Get(ctx, key)
					gl = append(gl, time.Since(start).Nanoseconds())
				} else {
					start := time.Now()
					_ = c.Set(ctx, key, val, time.Hour)
					sl = append(sl, time.Since(start).Nanoseconds())
				}
			}

			setSlices[id] = sl
			getSlices[id] = gl
		}(i)
	}

	wg.Wait()

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
	overseerAddr := flag.String("overseer", "localhost:9000", "Overseer gRPC address")
	concurrency := flag.Int("c", 50, "concurrent workers")
	dur := flag.Duration("d", 30*time.Second, "benchmark duration")
	keySpace := flag.Int("keys", 100_000, "key space size")
	valueSize := flag.Int("vsize", 256, "value size in bytes")
	rwRatio := flag.Float64("rw", 0.8, "fraction of operations that are GETs")
	flag.Parse()

	ctx := context.Background()
	val := strings.Repeat("x", *valueSize)

	c, err := client.New(ctx, *overseerAddr, client.WithTimeout(5*time.Second))
	if err != nil {
		fmt.Printf("connect: %v\n", err)
		return
	}
	defer c.Close()

	fmt.Printf("%-12s  %-3s  %10s  %8s  %8s\n", "system", "op", "ops/sec", "p50µs", "p99µs")
	fmt.Println(strings.Repeat("─", 50))

	fmt.Printf("[skylr @ %s]\n", *overseerAddr)
	warmup(ctx, c, *keySpace, val)
	r := runBench(ctx, c, *concurrency, *dur, *keySpace, val, *rwRatio)
	printRow("skylr", "SET", r.setLats, *dur)
	printRow("skylr", "GET", r.getLats, *dur)
}
