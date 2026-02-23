package hashring

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// ErrEmptyRing is returned by GetNode when no nodes have been added to the ring.
var ErrEmptyRing = errors.New("hash ring is empty")

// HashRing is a consistent hash ring with virtual nodes.
// All exported methods are safe for concurrent use.
type HashRing struct {
	mu         sync.RWMutex
	sorted     []uint32          // sorted virtual-node hash values
	ring       map[uint32]string // hash -> physical shard address
	nodes      map[string]struct{}
	vnodeCount int
}

// New creates a new HashRing.
// vnodeCount is the number of virtual nodes placed on the ring per physical node.
// Higher values improve key distribution at the cost of more memory.
func New(vnodeCount int) *HashRing {
	return &HashRing{
		ring:       make(map[uint32]string),
		nodes:      make(map[string]struct{}),
		vnodeCount: vnodeCount,
	}
}

// AddNode places vnodeCount virtual nodes for addr onto the ring.
// If addr is already present, this is a no-op.
func (hr *HashRing) AddNode(addr string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodes[addr]; exists {
		return
	}

	hr.nodes[addr] = struct{}{}

	for i := range hr.vnodeCount {
		h := hashKey(addr + "#" + strconv.Itoa(i))
		hr.ring[h] = addr
		hr.sorted = append(hr.sorted, h)
	}

	sort.Slice(hr.sorted, func(i, j int) bool {
		return hr.sorted[i] < hr.sorted[j]
	})
}

// RemoveNode removes all virtual nodes for addr from the ring.
// If addr is not present, this is a no-op.
func (hr *HashRing) RemoveNode(addr string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodes[addr]; !exists {
		return
	}

	delete(hr.nodes, addr)

	toRemove := make(map[uint32]struct{}, hr.vnodeCount)
	for i := range hr.vnodeCount {
		h := hashKey(addr + "#" + strconv.Itoa(i))
		toRemove[h] = struct{}{}
		delete(hr.ring, h)
	}

	filtered := hr.sorted[:0]
	for _, h := range hr.sorted {
		if _, shouldRemove := toRemove[h]; !shouldRemove {
			filtered = append(filtered, h)
		}
	}
	hr.sorted = filtered
}

// GetNode returns the physical node address responsible for the given key.
// It walks the ring clockwise from the key's hash position, wrapping around
// if necessary. Returns ErrEmptyRing if no nodes are registered.
func (hr *HashRing) GetNode(key string) (string, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sorted) == 0 {
		return "", ErrEmptyRing
	}

	h := hashKey(key)
	idx := sort.Search(len(hr.sorted), func(i int) bool {
		return hr.sorted[i] >= h
	})
	if idx >= len(hr.sorted) {
		idx = 0
	}

	return hr.ring[hr.sorted[idx]], nil
}

// Nodes returns a snapshot of all currently registered physical node addresses.
func (hr *HashRing) Nodes() []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	addrs := make([]string, 0, len(hr.nodes))
	for addr := range hr.nodes {
		addrs = append(addrs, addr)
	}
	return addrs
}

// Size returns the number of physical nodes currently on the ring.
func (hr *HashRing) Size() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodes)
}

// Snapshot returns a point-in-time read-only copy of the ring.
// The copy can be used without holding any lock and is safe for concurrent reads.
// It is intended for use during key migration so that the pre-addition state of
// the ring can be compared against the post-addition state.
func (hr *HashRing) Snapshot() *HashRing {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	sorted := make([]uint32, len(hr.sorted))
	copy(sorted, hr.sorted)

	ring := make(map[uint32]string, len(hr.ring))
	for k, v := range hr.ring {
		ring[k] = v
	}

	nodes := make(map[string]struct{}, len(hr.nodes))
	for k := range hr.nodes {
		nodes[k] = struct{}{}
	}

	return &HashRing{
		sorted:     sorted,
		ring:       ring,
		nodes:      nodes,
		vnodeCount: hr.vnodeCount,
	}
}

// hashKey computes a 32-bit FNV-1a hash of key.
func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
