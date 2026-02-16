package skiplist

import (
	"bytes"
	"math"
	"math/rand/v2"
	"sync/atomic"
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
// Keys are compared lexicographically (bytes.Compare semantics).
type Skiplist struct {
	arena  *arena
	head   uint32       // arena offset of head sentinel node
	height atomic.Int32 // current max height (1 ≤ height ≤ maxHeight)
}

// New creates a new skiplist backed by the given byte buffer.
func New(buf []byte) *Skiplist {
	a := newArena(buf)
	// Allocate head sentinel with max height, no key, no value.
	headOffset, err := newNode(a, maxHeight, nil, nil)
	if err != nil {
		panic("arena too small for head node")
	}

	s := &Skiplist{
		arena: a,
		head:  headOffset,
	}
	s.height.Store(1)
	return s
}

// Add inserts a key-value pair. If the key already exists (exact byte match),
// returns ErrRecordExists. If the arena is full, returns errArenaFull.
func (s *Skiplist) Add(key, value []byte) error {
	var spl [maxHeight]splice
	if s.findSplice(key, &spl) {
		return ErrRecordExists
	}

	height := s.randomHeight()
	ndOffset, err := newNode(s.arena, height, key, value)
	if err != nil {
		return err
	}

	// Grow the skiplist height if needed (CAS loop).
	listHeight := s.height.Load()
	for int32(height) > listHeight {
		if s.height.CompareAndSwap(listHeight, int32(height)) {
			break
		}
		listHeight = s.height.Load()
	}

	// Link the node at each level from bottom to top.
	for i := 0; i < int(height); i++ {
		for {
			prevOffset := spl[i].prev
			nextOffset := spl[i].next

			if prevOffset == 0 {
				// New level — this node is the first at this height.
				prevOffset = s.head
				nextOffset = 0
			}

			// Set new node's next pointer.
			nd := getNode(s.arena, ndOffset)
			nd.tower[i].nextOffset.Store(nextOffset)

			prev := getNode(s.arena, prevOffset)
			if prev.casNext(i, nextOffset, ndOffset) {
				break
			}

			// CAS failed — recompute splice at this level.
			prevOffset, nextOffset, found := s.findSpliceForLevel(key, i, prevOffset)
			if found {
				if i != 0 {
					panic("duplicate found at non-base level")
				}
				return ErrRecordExists
			}
			spl[i].prev = prevOffset
			spl[i].next = nextOffset
		}
	}

	return nil
}

// Get returns the value for the given key, or nil if not found.
func (s *Skiplist) Get(key []byte) ([]byte, bool) {
	_, nd, match := s.seekGE(key)
	if nd == 0 || !match {
		return nil, false
	}
	n := getNode(s.arena, nd)
	return n.getValue(s.arena), true
}

// Height returns the current max height.
func (s *Skiplist) Height() int {
	return int(s.height.Load())
}

// Size returns the number of bytes allocated from the arena.
func (s *Skiplist) Size() uint32 {
	return s.arena.size()
}

// splice records the (prev, next) offsets bracketing a key at one level.
type splice struct {
	prev uint32 // arena offset of the node before the key
	next uint32 // arena offset of the node at or after the key
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := rand.Uint32()
	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}
	return h
}

// findSplice finds the splice at every level for the given key.
// Returns true if an exact match is found.
func (s *Skiplist) findSplice(key []byte, spl *[maxHeight]splice) bool {
	prevOffset := s.head
	found := false

	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		nextOffset := getNode(s.arena, prevOffset).getNext(level)

		for nextOffset != 0 {
			next := getNode(s.arena, nextOffset)
			nextKey := next.getKey(s.arena)
			cmp := bytes.Compare(key, nextKey)
			if cmp <= 0 {
				if cmp == 0 {
					found = true
				}
				break
			}
			prevOffset = nextOffset
			nextOffset = next.getNext(level)
		}

		spl[level].prev = prevOffset
		spl[level].next = nextOffset
	}

	return found
}

// findSpliceForLevel recomputes the splice at a single level starting from start.
func (s *Skiplist) findSpliceForLevel(key []byte, level int, start uint32) (prev, next uint32, found bool) {
	prev = start
	next = getNode(s.arena, prev).getNext(level)

	for next != 0 {
		nd := getNode(s.arena, next)
		nextKey := nd.getKey(s.arena)
		cmp := bytes.Compare(key, nextKey)
		if cmp <= 0 {
			if cmp == 0 {
				found = true
			}
			return
		}
		prev = next
		next = nd.getNext(level)
	}
	return
}

// seekGE finds the first node with key >= target.
// Returns (prevOffset, nodeOffset, exactMatch).
func (s *Skiplist) seekGE(target []byte) (uint32, uint32, bool) {
	prevOffset := s.head
	var nextOffset uint32

	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		nextOffset = getNode(s.arena, prevOffset).getNext(level)

		for nextOffset != 0 {
			nd := getNode(s.arena, nextOffset)
			nextKey := nd.getKey(s.arena)
			cmp := bytes.Compare(target, nextKey)
			if cmp <= 0 {
				break
			}
			prevOffset = nextOffset
			nextOffset = nd.getNext(level)
		}
	}

	if nextOffset == 0 {
		return prevOffset, 0, false
	}
	nd := getNode(s.arena, nextOffset)
	match := bytes.Compare(target, nd.getKey(s.arena)) == 0
	return prevOffset, nextOffset, match
}
