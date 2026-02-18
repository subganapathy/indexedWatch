package skiplist

import (
	"bytes"
	"cmp"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"unsafe"
)

const pValue = 1 / math.E // Pebble uses 1/e (theoretically optimal)

var probabilities [maxHeight]uint32

func init() {
	p := float64(1.0)
	for i := range maxHeight {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

// Skiplist is a lock-free concurrent skiplist backed by an arena allocator.
// It supports concurrent reads and writes via CAS-based insertion.
// Deletion is not supported — tombstones are handled at a higher layer.
//
// Keys are ordered by (userKey ASC, trailer DESC). This means for the same
// user key, newer versions (higher seqNum) appear before older versions.
//
// Pebble reference: internal/arenaskl/skl.go
type Skiplist struct {
	arena  *arena
	head   *node
	tail   *node
	height atomic.Uint32 // current max height (1 ≤ height ≤ maxHeight)
}

// New creates a new skiplist backed by the given byte buffer.
func New(buf []byte) *Skiplist {
	a := newArena(buf)

	// Allocate head and tail sentinels with max height.
	headOffset, err := newNode(a, maxHeight, 0, nil, nil)
	if err != nil {
		panic("arena too small for head node")
	}
	tailOffset, err := newNode(a, maxHeight, 0, nil, nil)
	if err != nil {
		panic("arena too small for tail node")
	}

	head := (*node)(a.getPointer(headOffset))
	tail := (*node)(a.getPointer(tailOffset))

	// Link all head/tail levels together.
	headOff := a.getPointerOffset(unsafe.Pointer(head))
	tailOff := a.getPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset.Store(tailOff)
		tail.tower[i].prevOffset.Store(headOff)
	}

	s := &Skiplist{
		arena: a,
		head:  head,
		tail:  tail,
	}
	s.height.Store(1)
	return s
}

// Add inserts a key-value pair with the given trailer (seqNum + kind).
// If an exact internal key match exists (same userKey AND same trailer),
// returns ErrRecordExists. Multiple versions of the same userKey with
// different trailers coexist in the skiplist.
//
// If the arena is full, returns errArenaFull.
func (s *Skiplist) Add(key []byte, trailer Trailer, value []byte) error {
	var spl [maxHeight]splice
	if s.findSplice(key, trailer, &spl) {
		return ErrRecordExists
	}

	height := s.randomHeight()
	ndOffset, err := newNode(s.arena, height, trailer, key, value)
	if err != nil {
		return err
	}

	// Grow the skiplist height if needed (CAS loop).
	listHeight := s.height.Load()
	for height > listHeight {
		if s.height.CompareAndSwap(listHeight, height) {
			break
		}
		listHeight = s.height.Load()
	}

	// Link the node at each level from bottom to top.
	nd := (*node)(s.arena.getPointer(ndOffset))
	for i := 0; i < int(height); i++ {
		for {
			prev := spl[i].prev
			next := spl[i].next

			if prev == nil {
				// New level — node is the first at this height.
				prev = s.head
				next = s.tail
			}

			// Initialize this node's pointers at this level.
			prevOffset := s.nodeOffset(prev)
			nextOffset := s.nodeOffset(next)
			nd.tower[i].init(prevOffset, nextOffset)

			// Help fix stale prev pointers from concurrent inserts.
			// If next.prev != prev, either:
			//   1. Another inserter hasn't finished linking (help it)
			//   2. A new node was inserted between prev and next
			nextPrevOff := next.getPrev(i)
			if nextPrevOff != prevOffset {
				prevNextOff := prev.getNext(i)
				if prevNextOff == nextOffset {
					// Case 1: help the other thread.
					next.casPrev(i, nextPrevOff, prevOffset)
				}
			}

			// CAS prev.next from next to nd.
			if prev.casNext(i, nextOffset, ndOffset) {
				// Success. Now update next.prev from prev to nd.
				next.casPrev(i, prevOffset, ndOffset)
				break
			}

			// CAS failed — recompute splice at this level.
			p, n, found := s.findSpliceForLevel(key, trailer, i, prev)
			if found {
				if i != 0 {
					panic("duplicate found at non-base level")
				}
				return ErrRecordExists
			}
			spl[i].prev = p
			spl[i].next = n
		}
	}

	return nil
}

// Height returns the current max height.
func (s *Skiplist) Height() uint32 {
	return s.height.Load()
}

// Size returns the number of bytes allocated from the arena.
func (s *Skiplist) Size() uint32 {
	return s.arena.size()
}

// splice records the (prev, next) nodes bracketing a key at one level.
type splice struct {
	prev *node
	next *node
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := rand.Uint32()
	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}
	return h
}

// compareKeys compares internal keys: (userKey ASC, trailer DESC).
// For equal user keys, higher trailers sort first (newer versions first).
func compareKeys(aKey []byte, aTrailer Trailer, bKey []byte, bTrailer Trailer) int {
	if x := bytes.Compare(aKey, bKey); x != 0 {
		return x
	}
	// Reverse order for trailer: higher trailers sort first.
	return cmp.Compare(bTrailer, aTrailer)
}

// keyIsAfterNode returns true if the given key sorts after nd's key.
func (s *Skiplist) keyIsAfterNode(nd *node, key []byte, trailer Trailer) bool {
	ndKey := nd.getKey(s.arena)
	x := bytes.Compare(ndKey, key)
	if x < 0 {
		return true
	}
	if x > 0 {
		return false
	}
	// User keys equal — reverse trailer comparison.
	return trailer < nd.keyTrailer
}

// findSplice finds the splice at every level for the given key+trailer.
// Returns true if an exact internal key match is found.
func (s *Skiplist) findSplice(key []byte, trailer Trailer, spl *[maxHeight]splice) bool {
	prev := s.head
	found := false

	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		next := s.getNext(prev, level)

		for next != s.tail {
			cmp := compareKeys(key, trailer, next.getKey(s.arena), next.keyTrailer)
			if cmp <= 0 {
				if cmp == 0 {
					found = true
				}
				break
			}
			prev = next
			next = s.getNext(prev, level)
		}

		spl[level].prev = prev
		spl[level].next = next
	}

	return found
}

// findSpliceForLevel recomputes the splice at a single level starting from start.
func (s *Skiplist) findSpliceForLevel(key []byte, trailer Trailer, level int, start *node) (prev, next *node, found bool) {
	prev = start
	next = s.getNext(prev, level)

	for next != s.tail {
		cmp := compareKeys(key, trailer, next.getKey(s.arena), next.keyTrailer)
		if cmp <= 0 {
			if cmp == 0 {
				found = true
			}
			return
		}
		prev = next
		next = s.getNext(prev, level)
	}
	return
}

// seekGE finds the first node with internal key >= (target, trailer).
// Returns the node and whether it was an exact userKey match.
func (s *Skiplist) seekGE(target []byte, trailer Trailer) (*node, bool) {
	prev := s.head
	var next *node

	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		next = s.getNext(prev, level)

		for next != s.tail {
			cmp := compareKeys(target, trailer, next.getKey(s.arena), next.keyTrailer)
			if cmp <= 0 {
				break
			}
			prev = next
			next = s.getNext(prev, level)
		}
	}

	if next == s.tail {
		return nil, false
	}
	match := bytes.Equal(target, next.getKey(s.arena))
	return next, match
}

// seekForBaseSplice finds the (prev, next) at level 0 where prev.key < key <= next.key.
// Used by SeekLT to find the node just before the target.
func (s *Skiplist) seekForBaseSplice(target []byte) (*node, *node) {
	prev := s.head
	var next *node

	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		next = s.getNext(prev, level)

		for next != s.tail {
			// For SeekLT, we compare by userKey only (find first node >= target userKey).
			cmp := bytes.Compare(target, next.getKey(s.arena))
			if cmp <= 0 {
				break
			}
			prev = next
			next = s.getNext(prev, level)
		}
	}

	return prev, next
}

// nodeOffset returns the arena offset for a node pointer.
func (s *Skiplist) nodeOffset(nd *node) uint32 {
	return s.arena.getPointerOffset(unsafe.Pointer(nd))
}

// getNext returns the next node at the given level.
func (s *Skiplist) getNext(nd *node, h int) *node {
	offset := nd.tower[h].nextOffset.Load()
	return (*node)(s.arena.getPointer(offset))
}

// getPrev returns the previous node at the given level.
func (s *Skiplist) getPrev(nd *node, h int) *node {
	offset := nd.tower[h].prevOffset.Load()
	return (*node)(s.arena.getPointer(offset))
}
