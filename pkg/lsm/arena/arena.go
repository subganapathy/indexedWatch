// Package arena provides a lock-free bump-pointer allocator on a single []byte
// slab. Skiplist nodes are allocated from here, avoiding per-node GC pressure.
//
// Allocations return uint32 offsets (not pointers) — compact and atomically
// loadable. There is no free: the entire arena is freed as a unit when the
// memtable flushes to an SSTable.
//
// Thread-safe via atomic.Uint64 on the allocation offset (no mutex on hot path).
//
// Pebble reference: internal/arenaskl/arena.go
package arena

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

// ErrFull is returned when the arena cannot satisfy an allocation.
var ErrFull = errors.New("arena: full")

const (
	// NodeAlignment is the minimum alignment for skiplist node allocations.
	// Forward pointers are uint32, so 4-byte alignment suffices.
	NodeAlignment = 4

	// maxSize is the maximum arena size, bounded by uint32 offset space.
	maxSize = 1<<32 - 1
)

// Arena is a lock-free bump-pointer allocator backed by a single []byte slab.
// Offset 0 is reserved as a nil sentinel — no data is stored there.
type Arena struct {
	n   atomic.Uint64
	buf []byte
}

// New creates an arena with the given capacity.
// The caller provides the backing buffer; the arena does not allocate.
func New(buf []byte) *Arena {
	if len(buf) > maxSize {
		buf = buf[:maxSize]
	}
	a := &Arena{buf: buf}
	// Reserve offset 0 as nil sentinel.
	a.n.Store(1)
	return a
}

// Alloc allocates size bytes with the given alignment (must be a power of 2).
// Returns the offset into the arena where the allocation starts.
// Returns ErrFull if there is not enough space.
func (a *Arena) Alloc(size, alignment uint32) (uint32, error) {
	// Pad to ensure alignment.
	padded := uint64(size) + uint64(alignment) - 1
	newSize := a.n.Add(padded)
	if newSize > uint64(len(a.buf)) {
		return 0, ErrFull
	}
	// Align the offset.
	offset := (uint32(newSize) - size) & ^(alignment - 1)
	return offset, nil
}

// GetBytes returns a slice of the arena at [offset, offset+size).
// Returns nil for offset 0 (nil sentinel).
func (a *Arena) GetBytes(offset, size uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size : offset+size]
}

// PutBytes copies src into the arena at offset. The caller must have
// allocated at least len(src) bytes starting at offset.
func (a *Arena) PutBytes(offset uint32, src []byte) {
	copy(a.buf[offset:offset+uint32(len(src))], src)
}

// Size returns the number of bytes allocated (including the reserved byte 0).
func (a *Arena) Size() uint32 {
	s := a.n.Load()
	if s > uint64(len(a.buf)) {
		return uint32(len(a.buf))
	}
	return uint32(s)
}

// Capacity returns the total capacity of the arena.
func (a *Arena) Capacity() uint32 {
	return uint32(len(a.buf))
}

// Pointer returns an unsafe.Pointer into the arena at the given offset.
// Unlike GetBytes, this does not create a sub-slice, so the resulting pointer
// is recognized by checkptr as belonging to the full backing allocation.
// This is needed for unsafe casts to structs (e.g., skiplist nodes).
func (a *Arena) Pointer(offset uint32) unsafe.Pointer {
	return unsafe.Pointer(&a.buf[offset])
}

// Reset resets the arena for reuse, keeping the underlying buffer.
// Not safe for concurrent use — only call when no readers exist.
func (a *Arena) Reset() {
	a.n.Store(1)
}
