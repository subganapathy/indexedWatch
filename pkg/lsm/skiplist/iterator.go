package skiplist

import "github.com/subganapathy/indexedwatch/pkg/lsm/arena"

// Iterator is a forward iterator over the skiplist.
// Zero value is not valid — use Skiplist.NewIterator.
type Iterator struct {
	list  *Skiplist
	arena *arena.Arena
	nd    uint32 // current node offset (0 = invalid)
}

// NewIterator returns a new iterator positioned before the first element.
// Call SeekGE or First to position the iterator.
func (s *Skiplist) NewIterator() *Iterator {
	return &Iterator{
		list:  s,
		arena: s.arena,
	}
}

// Valid returns true if the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.nd != 0
}

// SeekGE positions the iterator at the first key >= target.
// Returns true if the iterator is valid after the seek.
func (it *Iterator) SeekGE(target []byte) bool {
	_, it.nd, _ = it.list.seekGE(target)
	return it.nd != 0
}

// First positions the iterator at the first element.
func (it *Iterator) First() bool {
	head := getNode(it.arena, it.list.head)
	it.nd = head.getNext(0)
	return it.nd != 0
}

// Next advances the iterator to the next element.
// Returns true if the iterator is valid after advancing.
func (it *Iterator) Next() bool {
	if it.nd == 0 {
		return false
	}
	nd := getNode(it.arena, it.nd)
	it.nd = nd.getNext(0)
	return it.nd != 0
}

// Key returns the key at the current position.
// Only valid when Valid() returns true.
func (it *Iterator) Key() []byte {
	nd := getNode(it.arena, it.nd)
	return nd.getKey(it.arena)
}

// Value returns the value at the current position.
// Only valid when Valid() returns true.
func (it *Iterator) Value() []byte {
	nd := getNode(it.arena, it.nd)
	return nd.getValue(it.arena)
}
