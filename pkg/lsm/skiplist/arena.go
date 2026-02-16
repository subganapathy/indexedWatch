// arena implements a lock-free bump-pointer allocator on a single []byte slab.
// Skiplist nodes are allocated from here, avoiding per-node GC pressure.
//
// Allocations return uint32 offsets (not pointers) — compact and atomically
// loadable. There is no free: the entire arena is freed as a unit when the
// memtable flushes to an SSTable.
//
// Thread-safe via atomic.Uint64 on the allocation offset (no mutex on hot path).
//
// Pebble reference: internal/arenaskl/arena.go
package skiplist

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

// errArenaFull is returned when the arena cannot satisfy an allocation.
var errArenaFull = errors.New("arena: full")

const (
	// nodeAlignment is the minimum alignment for skiplist node allocations.
	// Forward pointers are uint32, so 4-byte alignment suffices.
	nodeAlignment = 4

	// maxArenaSize is the maximum arena size, bounded by uint32 offset space.
	maxArenaSize = 1<<32 - 1
)

// arena is a lock-free bump-pointer allocator backed by a single []byte slab.
// Offset 0 is reserved as a nil sentinel — no data is stored there.
type arena struct {
	n   atomic.Uint64
	buf []byte
}

// newArena creates an arena with the given backing buffer.
// The caller provides the buffer; the arena does not allocate.
func newArena(buf []byte) *arena {
	if len(buf) > maxArenaSize {
		buf = buf[:maxArenaSize]
	}
	a := &arena{buf: buf}
	// Reserve offset 0 as nil sentinel.
	a.n.Store(1)
	return a
}

// alloc allocates size bytes with the given alignment (must be a power of 2).
// Returns the offset into the arena where the allocation starts.
//
// If overflow is not 0, it also ensures that many bytes after the allocation
// exist within the arena. This is used for skiplist nodes where the struct
// cast (unsafe.Pointer → *node) interprets more bytes than are actually
// allocated — the unused upper tower levels overlap with the next allocation
// but are never accessed. The overflow check prevents the struct from
// extending past the end of the arena buffer.
//
// Returns errArenaFull if there is not enough space.
func (a *arena) alloc(size, alignment, overflow uint32) (uint32, error) {
	// Verify that the arena isn't already full. A previous allocation may
	// have bumped n past len(buf). This avoids an unnecessary atomic Add
	// on a dead arena.
	origSize := a.n.Load()
	if int(origSize) > len(a.buf) {
		return 0, errArenaFull
	}

	// Pad to ensure alignment.
	padded := uint64(size) + uint64(alignment) - 1
	newSize := a.n.Add(padded)
	if newSize+uint64(overflow) > uint64(len(a.buf)) {
		return 0, errArenaFull
	}
	// Align the offset.
	offset := (uint32(newSize) - size) & ^(alignment - 1)
	return offset, nil
}

// getBytes returns a slice of the arena at [offset, offset+size).
// Returns nil for offset 0 (nil sentinel).
func (a *arena) getBytes(offset, size uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size : offset+size]
}

// size returns the number of bytes allocated (including the reserved byte 0).
func (a *arena) size() uint32 {
	s := a.n.Load()
	if s > uint64(len(a.buf)) {
		return uint32(len(a.buf))
	}
	return uint32(s)
}

// capacity returns the total capacity of the arena.
func (a *arena) capacity() uint32 {
	return uint32(len(a.buf))
}

// getPointer returns an unsafe.Pointer into the arena at the given offset.
// Unlike getBytes, this does not create a sub-slice, so the resulting pointer
// is recognized by checkptr as belonging to the full backing allocation.
// This is needed for unsafe casts to structs (e.g., skiplist nodes).
func (a *arena) getPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	return unsafe.Pointer(&a.buf[offset])
}

// getPointerOffset returns the arena offset for a pointer that points into
// the arena's buffer. Used to convert a *node back to a uint32 offset for
// storage in other nodes' tower links.
func (a *arena) getPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}
	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}

// reset resets the arena for reuse, keeping the underlying buffer.
// Not safe for concurrent use — only call when no readers exist.
func (a *arena) reset() {
	a.n.Store(1)
}
