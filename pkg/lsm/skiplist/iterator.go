package skiplist

// Iterator is a bidirectional iterator over the skiplist.
// Zero value is not valid — use Skiplist.NewIterator.
//
// Pebble reference: internal/arenaskl/iterator.go
type Iterator struct {
	list *Skiplist
	nd   *node // current node (nil = invalid)
}

// NewIterator returns a new iterator positioned before the first element.
// Call SeekGE, SeekLT, First, or Last to position the iterator.
func (s *Skiplist) NewIterator() *Iterator {
	return &Iterator{list: s}
}

// Valid returns true if the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.nd != nil && it.nd != it.list.head && it.nd != it.list.tail
}

// First positions the iterator at the first element.
func (it *Iterator) First() bool {
	it.nd = it.list.getNext(it.list.head, 0)
	return it.Valid()
}

// Last positions the iterator at the last element.
func (it *Iterator) Last() bool {
	it.nd = it.list.getPrev(it.list.tail, 0)
	return it.Valid()
}

// SeekGE positions the iterator at the first internal key >= (target, trailer).
// Returns true if the iterator is valid after the seek.
func (it *Iterator) SeekGE(target []byte) bool {
	// Use max trailer so we find the first node with userKey >= target
	// (since ordering is userKey ASC, trailer DESC, max trailer means
	// "the very first version of this key or later").
	nd, _ := it.list.seekGE(target, Trailer(^uint64(0)))
	it.nd = nd
	return it.Valid()
}

// SeekLT positions the iterator at the last entry with userKey < target.
// Returns true if the iterator is valid after the seek.
func (it *Iterator) SeekLT(target []byte) bool {
	// seekForBaseSplice finds (prev, next) where prev < target <= next.
	prev, _ := it.list.seekForBaseSplice(target)
	it.nd = prev
	return it.Valid()
}

// Next advances the iterator to the next element.
// Returns true if the iterator is valid after advancing.
func (it *Iterator) Next() bool {
	if it.nd == nil {
		return false
	}
	it.nd = it.list.getNext(it.nd, 0)
	return it.Valid()
}

// Prev moves the iterator to the previous element.
// Returns true if the iterator is valid after moving.
func (it *Iterator) Prev() bool {
	if it.nd == nil {
		return false
	}
	it.nd = it.list.getPrev(it.nd, 0)
	return it.Valid()
}

// Key returns the user key at the current position.
// Only valid when Valid() returns true.
func (it *Iterator) Key() []byte {
	return it.nd.getKey(it.list.arena)
}

// Trailer returns the trailer (seqNum + kind) at the current position.
// Only valid when Valid() returns true.
func (it *Iterator) Trailer() Trailer {
	return it.nd.keyTrailer
}

// Value returns the value at the current position.
// Only valid when Valid() returns true.
func (it *Iterator) Value() []byte {
	return it.nd.getValue(it.list.arena)
}
