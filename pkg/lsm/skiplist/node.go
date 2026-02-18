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

// links holds forward and backward pointers (arena offsets) at one level.
type links struct {
	nextOffset atomic.Uint32
	prevOffset atomic.Uint32
}

func (l *links) init(prevOffset, nextOffset uint32) {
	l.nextOffset.Store(nextOffset)
	l.prevOffset.Store(prevOffset)
}

// node is the skiplist node stored in the arena.
// The tower array is truncated to the actual height when allocated —
// unused levels beyond the node's height are never accessed (their memory
// overlaps with the next allocation via the arena's overflow mechanism).
//
// Pebble reference: internal/arenaskl/node.go
type node struct {
	keyOffset  uint32
	keySize    uint32
	keyTrailer Trailer
	valueSize  uint32

	// Padding to align tower on an 8-byte boundary, so that 32-bit and
	// 64-bit architectures use the same memory layout for node.
	_ [4]byte

	// tower holds forward and backward pointers at each level [0, height).
	// Accessed atomically for concurrent reads and CAS-based insertion.
	tower [maxHeight]links
}

// newNode allocates a node in the arena and copies in the key and value.
// The node struct is truncated to the actual height — unused tower levels
// are not allocated. The overflow parameter ensures the unsafe.Pointer cast
// to *node doesn't extend past the arena buffer.
func newNode(a *arena, height uint32, trailer Trailer, key, value []byte) (uint32, error) {
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
	nd.keyTrailer = trailer
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

// getPrev returns the prev node offset at the given level.
func (n *node) getPrev(level int) uint32 {
	return n.tower[level].prevOffset.Load()
}

// casNext atomically compares-and-swaps the next pointer at the given level.
func (n *node) casNext(level int, old, new uint32) bool {
	return n.tower[level].nextOffset.CompareAndSwap(old, new)
}

// casPrev atomically compares-and-swaps the prev pointer at the given level.
func (n *node) casPrev(level int, old, new uint32) bool {
	return n.tower[level].prevOffset.CompareAndSwap(old, new)
}
