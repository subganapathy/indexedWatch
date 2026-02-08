package lsm

import "container/heap"

// MergeIterator merges multiple sorted iterators into a single sorted stream.
// For identical keys, the iterator from the source with the lowest index wins
// (index 0 = memtable = newest data, higher index = older data/deeper levels).
type MergeIterator struct {
	iters []Iterator
	h     iterHeap
	dir   int // 1 = forward, -1 = reverse, 0 = not positioned
}

// Iterator is the interface that all LSM iterator sources must implement.
type Iterator interface {
	SeekGE(key []byte) bool
	First() bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
}

type iterItem struct {
	index int
	iter  Iterator
}

type iterHeap []iterItem

func (h iterHeap) Len() int { return len(h) }
func (h iterHeap) Less(i, j int) bool {
	cmp := compareKeys(h[i].iter.Key(), h[j].iter.Key())
	if cmp != 0 {
		return cmp < 0
	}
	// Same key: prefer the iterator with lower index (newer data).
	return h[i].index < h[j].index
}
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x any) {
	*h = append(*h, x.(iterItem))
}
func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// NewMergeIterator creates a merge iterator over the given sources.
// Sources should be ordered from newest to oldest (index 0 = newest).
func NewMergeIterator(iters []Iterator) *MergeIterator {
	return &MergeIterator{iters: iters}
}

// SeekGE positions at the first key >= target across all sources.
func (m *MergeIterator) SeekGE(key []byte) bool {
	m.h = m.h[:0]
	for i, it := range m.iters {
		if it.SeekGE(key) {
			heap.Push(&m.h, iterItem{index: i, iter: it})
		}
	}
	m.dir = 1
	return len(m.h) > 0
}

// First positions at the smallest key across all sources.
func (m *MergeIterator) First() bool {
	m.h = m.h[:0]
	for i, it := range m.iters {
		if it.First() {
			heap.Push(&m.h, iterItem{index: i, iter: it})
		}
	}
	m.dir = 1
	return len(m.h) > 0
}

// Next advances to the next key. Skips duplicate keys from older sources.
func (m *MergeIterator) Next() bool {
	if len(m.h) == 0 {
		return false
	}

	// Save current key to skip duplicates.
	currentKey := m.h[0].iter.Key()
	prevKey := make([]byte, len(currentKey))
	copy(prevKey, currentKey)

	// Advance all iterators that are at the current key.
	for len(m.h) > 0 && compareKeys(m.h[0].iter.Key(), prevKey) == 0 {
		top := m.h[0]
		if top.iter.Next() {
			heap.Fix(&m.h, 0)
		} else {
			heap.Pop(&m.h)
		}
	}

	return len(m.h) > 0
}

// Valid returns whether the iterator is positioned at a valid entry.
func (m *MergeIterator) Valid() bool {
	return len(m.h) > 0
}

// Key returns the current key.
func (m *MergeIterator) Key() []byte {
	return m.h[0].iter.Key()
}

// Value returns the current value (from the newest source for this key).
func (m *MergeIterator) Value() []byte {
	return m.h[0].iter.Value()
}
