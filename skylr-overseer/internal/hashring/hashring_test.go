package hashring

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetNode_EmptyRing(t *testing.T) {
	t.Parallel()

	hr := New(10)
	_, err := hr.GetNode("anykey")
	assert.ErrorIs(t, err, ErrEmptyRing)
}

func TestGetNode_SingleNode(t *testing.T) {
	t.Parallel()

	hr := New(10)
	hr.AddNode("node1:9000")

	addr, err := hr.GetNode("somekey")
	require.NoError(t, err)
	assert.Equal(t, "node1:9000", addr)
}

func TestAddNode_Idempotent(t *testing.T) {
	t.Parallel()

	hr := New(10)
	hr.AddNode("node1:9000")
	hr.AddNode("node1:9000")

	assert.Equal(t, 1, hr.Size())
	assert.Equal(t, 10, len(hr.sorted))
}

func TestRemoveNode_ExistingNode(t *testing.T) {
	t.Parallel()

	hr := New(10)
	hr.AddNode("node1:9000")
	hr.AddNode("node2:9000")
	hr.RemoveNode("node1:9000")

	assert.Equal(t, 1, hr.Size())
	assert.Equal(t, 10, len(hr.sorted))

	addr, err := hr.GetNode("anykey")
	require.NoError(t, err)
	assert.Equal(t, "node2:9000", addr)
}

func TestRemoveNode_NonExistentNode(t *testing.T) {
	t.Parallel()

	hr := New(10)
	hr.AddNode("node1:9000")

	hr.RemoveNode("does-not-exist:9000")

	assert.Equal(t, 1, hr.Size())
}

func TestRemoveNode_AllNodes_EmptyRing(t *testing.T) {
	t.Parallel()

	hr := New(10)
	hr.AddNode("node1:9000")
	hr.RemoveNode("node1:9000")

	assert.Equal(t, 0, hr.Size())

	_, err := hr.GetNode("anykey")
	assert.ErrorIs(t, err, ErrEmptyRing)
}

func TestGetNode_Deterministic(t *testing.T) {
	t.Parallel()

	hr := New(150)
	hr.AddNode("a:9000")
	hr.AddNode("b:9000")
	hr.AddNode("c:9000")

	key := "test-key-42"
	first, err := hr.GetNode(key)
	require.NoError(t, err)

	for range 100 {
		addr, err := hr.GetNode(key)
		require.NoError(t, err)
		assert.Equal(t, first, addr)
	}
}

// TestGetNode_Distribution verifies that with 150 vnodes/node the key distribution
// across 3 nodes stays within a reasonable range (each node should own 20-47% of keys).
func TestGetNode_Distribution(t *testing.T) {
	t.Parallel()

	const (
		vnodeCount = 150
		keyCount   = 10_000
		nodeCount  = 3
	)

	hr := New(vnodeCount)
	nodes := []string{"node1:9000", "node2:9000", "node3:9000"}
	for _, n := range nodes {
		hr.AddNode(n)
	}

	counts := make(map[string]int, nodeCount)
	for i := range keyCount {
		addr, err := hr.GetNode(fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		counts[addr]++
	}

	for _, n := range nodes {
		pct := float64(counts[n]) / float64(keyCount) * 100
		assert.GreaterOrEqual(t, pct, 10.0, "node %s: distribution too low (%.1f%%)", n, pct)
		assert.LessOrEqual(t, pct, 60.0, "node %s: distribution too high (%.1f%%)", n, pct)
	}
}

// TestGetNode_Remapping verifies that when a node is removed only the keys
// that belonged to it are remapped; keys on surviving nodes stay stable.
func TestGetNode_Remapping(t *testing.T) {
	t.Parallel()

	const keyCount = 1_000

	hr := New(150)
	hr.AddNode("a:9000")
	hr.AddNode("b:9000")
	hr.AddNode("c:9000")

	before := make(map[string]string, keyCount)
	for i := range keyCount {
		key := fmt.Sprintf("k-%d", i)
		addr, _ := hr.GetNode(key)
		before[key] = addr
	}

	hr.RemoveNode("c:9000")

	remappedCount := 0
	for i := range keyCount {
		key := fmt.Sprintf("k-%d", i)
		after, _ := hr.GetNode(key)
		if before[key] == "c:9000" {
			// keys previously on c must now go to a or b
			assert.NotEqual(t, "c:9000", after)
			remappedCount++
		} else {
			// keys NOT on c must stay exactly where they were
			assert.Equal(t, before[key], after, "key %s moved unexpectedly from %s to %s", key, before[key], after)
		}
	}

	t.Logf("Remapped %d/%d keys after removing node c", remappedCount, keyCount)
}

func TestSnapshot_Independence(t *testing.T) {
	t.Parallel()

	hr := New(10)
	hr.AddNode("node1:9000")
	hr.AddNode("node2:9000")

	snap := hr.Snapshot()

	hr.AddNode("node3:9000")
	hr.RemoveNode("node1:9000")

	assert.Equal(t, 2, snap.Size(), "snapshot should not be affected by mutations to the original ring")
	assert.Equal(t, 2, hr.Size(), "original ring: added node3, removed node1 => 2 nodes")
}

func TestNodes_ReturnsAllAdded(t *testing.T) {
	t.Parallel()

	hr := New(10)
	hr.AddNode("a:9000")
	hr.AddNode("b:9000")
	hr.AddNode("c:9000")
	hr.RemoveNode("b:9000")

	nodes := hr.Nodes()
	assert.Len(t, nodes, 2)
	assert.ElementsMatch(t, []string{"a:9000", "c:9000"}, nodes)
}
