// Package skiplist implements a lock-free concurrent skiplist built on an arena
// allocator. Supports concurrent reads and writes via CAS on arena offsets.
//
// Nodes are allocated from the arena: key/value offsets+lengths, height, and a
// forward pointer array. The skiplist uses lexicographic byte comparison.
//
// Pebble reference: internal/arenaskl/skl.go, node.go, iterator.go
package skiplist

import (
	"sync/atomic"
	"unsafe"

	"github.com/subganapathy/indexedwatch/pkg/lsm/arena"
)

const (
	// MaxHeight is the maximum tower height for any node.
	MaxHeight = 20

	maxNodeSize = int(unsafe.Sizeof(node{}))
)

// links holds a single forward pointer (arena offset) at one level.
type links struct {
	nextOffset atomic.Uint32
}

// node is the skiplist node stored in the arena.
// The tower array is truncated to the actual height when allocated —
// unused levels beyond the node's height are never accessed.
type node struct {
	keyOffset   uint32
	keySize     uint32
	valueOffset uint32
	valueSize   uint32

	// tower holds forward pointers at each level [0, height).
	// Accessed atomically for concurrent reads and CAS-based insertion.
	tower [MaxHeight]links
}

// newNode allocates a node in the arena and copies in the key and value.
func newNode(a *arena.Arena, height uint32, key, value []byte) (uint32, error) {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	// Always allocate the full maxNodeSize for the node struct so the
	// unsafe.Pointer cast to *node is valid for checkptr — even though we
	// only use tower[0:height], the cast interprets sizeof(node) bytes.
	nodeSize := uint32(maxNodeSize)

	offset, err := a.Alloc(nodeSize+keySize+valueSize, arena.NodeAlignment)
	if err != nil {
		return 0, err
	}

	nd := (*node)(a.Pointer(offset))
	nd.keyOffset = offset + nodeSize
	nd.keySize = keySize
	nd.valueOffset = offset + nodeSize + keySize
	nd.valueSize = valueSize

	a.PutBytes(nd.keyOffset, key)
	a.PutBytes(nd.valueOffset, value)

	return offset, nil
}

// getNode returns a *node from an arena offset.
func getNode(a *arena.Arena, offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(a.Pointer(offset))
}

// getKey returns the key bytes for this node.
func (n *node) getKey(a *arena.Arena) []byte {
	return a.GetBytes(n.keyOffset, n.keySize)
}

// getValue returns the value bytes for this node.
func (n *node) getValue(a *arena.Arena) []byte {
	return a.GetBytes(n.valueOffset, n.valueSize)
}

// getNext returns the next node offset at the given level.
func (n *node) getNext(level int) uint32 {
	return n.tower[level].nextOffset.Load()
}

// casNext atomically compares-and-swaps the next pointer at the given level.
func (n *node) casNext(level int, old, new uint32) bool {
	return n.tower[level].nextOffset.CompareAndSwap(old, new)
}
