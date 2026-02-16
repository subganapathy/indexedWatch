package skiplist

import (
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight   = 20
	maxNodeSize = int(unsafe.Sizeof(node{}))
	linksSize   = int(unsafe.Sizeof(links{}))
)

// links holds a single forward pointer (arena offset) at one level.
type links struct {
	nextOffset atomic.Uint32
}

// node is the skiplist node stored in the arena.
// The tower array is truncated to the actual height when allocated —
// unused levels beyond the node's height are never accessed (their memory
// overlaps with the next allocation via the arena's overflow mechanism).
type node struct {
	keyOffset uint32
	keySize   uint32
	valueSize uint32

	// tower holds forward pointers at each level [0, height).
	// Accessed atomically for concurrent reads and CAS-based insertion.
	tower [maxHeight]links
}

// newNode allocates a node in the arena and copies in the key and value.
// The node struct is truncated to the actual height — unused tower levels
// are not allocated. The overflow parameter ensures the unsafe.Pointer cast
// to *node doesn't extend past the arena buffer.
func newNode(a *arena, height uint32, key, value []byte) (uint32, error) {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	unusedSize := uint32((maxHeight - int(height)) * linksSize)
	nodeSize := uint32(maxNodeSize) - unusedSize

	offset, err := a.alloc(nodeSize+keySize+valueSize, nodeAlignment, unusedSize)
	if err != nil {
		return 0, err
	}

	nd := (*node)(a.getPointer(offset))
	nd.keyOffset = offset + nodeSize
	nd.keySize = keySize
	nd.valueSize = valueSize

	copy(a.getBytes(nd.keyOffset, keySize), key)
	copy(a.getBytes(nd.keyOffset+keySize, valueSize), value)

	return offset, nil
}

// getNode returns a *node from an arena offset.
func getNode(a *arena, offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(a.getPointer(offset))
}

// getKey returns the key bytes for this node.
func (n *node) getKey(a *arena) []byte {
	return a.getBytes(n.keyOffset, n.keySize)
}

// getValue returns the value bytes for this node.
func (n *node) getValue(a *arena) []byte {
	return a.getBytes(n.keyOffset+n.keySize, n.valueSize)
}

// getNext returns the next node offset at the given level.
func (n *node) getNext(level int) uint32 {
	return n.tower[level].nextOffset.Load()
}

// casNext atomically compares-and-swaps the next pointer at the given level.
func (n *node) casNext(level int, old, new uint32) bool {
	return n.tower[level].nextOffset.CompareAndSwap(old, new)
}
