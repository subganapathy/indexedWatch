package memtable

import (
	"sync/atomic"

	"github.com/subganapathy/indexedwatch/pkg/lsm/arena"
	"github.com/subganapathy/indexedwatch/pkg/lsm/skiplist"
)

// Memtable is an in-memory sorted store backed by a skiplist on an arena.
// All writes go here first. When the arena is full, the memtable is "sealed"
// and a new one is created, while the sealed one is flushed to an SSTable.
//
// The memtable encodes keys as internal keys (userKey + seqNum + kind),
// enabling MVCC: multiple versions of the same user key can coexist,
// ordered by descending sequence number (newest first).
type Memtable struct {
	skl   *skiplist.Skiplist
	arena *arena.Arena

	// Sequence number range of entries in this memtable.
	minSeqNum atomic.Uint64
	maxSeqNum atomic.Uint64

	// refs tracks the number of active references (iterators + 1 for the memtable itself).
	// When refs drops to 0 the memtable can be freed.
	refs atomic.Int32
}

// New creates a new Memtable with the given arena size.
func New(arenaSize int) *Memtable {
	a := arena.New(make([]byte, arenaSize))
	m := &Memtable{
		skl:   skiplist.New(a),
		arena: a,
	}
	m.refs.Store(1)
	return m
}

// Set inserts or updates a key-value pair with the given sequence number.
// Returns arena.ErrFull if the memtable is full.
func (m *Memtable) Set(key, value []byte, seqNum uint64) error {
	ikey := MakeInternalKey(key, seqNum, KindSet)
	m.updateSeqRange(seqNum)
	return m.skl.Add(ikey, value)
}

// Delete inserts a tombstone for the key with the given sequence number.
// Returns arena.ErrFull if the memtable is full.
func (m *Memtable) Delete(key []byte, seqNum uint64) error {
	ikey := MakeInternalKey(key, seqNum, KindDelete)
	m.updateSeqRange(seqNum)
	return m.skl.Add(ikey, nil)
}

// Get looks up the most recent value for the user key that is visible at
// the given sequence number. Returns the value and true if found, or nil
// and false if not found or if the most recent entry is a tombstone.
func (m *Memtable) Get(key []byte, seqNum uint64) ([]byte, bool) {
	// Seek to the first internal key >= (key, seqNum).
	// Since internal keys for the same userKey are ordered newest-first,
	// the seek target with the given seqNum will land on the newest visible version.
	seekKey := MakeInternalKey(key, seqNum, KindSet)
	it := m.skl.NewIterator()
	if !it.SeekGE(seekKey) {
		return nil, false
	}

	// Check if the found key matches our user key.
	foundUserKey, _, kind := ParseInternalKey(it.Key())
	if !bytesEqual(foundUserKey, key) {
		return nil, false
	}

	if kind == KindDelete {
		return nil, false
	}
	return it.Value(), true
}

// NewIterator returns a new iterator over the memtable's internal keys.
// The iterator yields keys in sorted order (userKey ascending, seqNum descending).
func (m *Memtable) NewIterator() *Iterator {
	return &Iterator{skl: m.skl.NewIterator()}
}

// ApproxSize returns the approximate number of bytes used in the arena.
func (m *Memtable) ApproxSize() uint32 {
	return m.arena.Size()
}

// IsFull returns true if the arena is more than 95% full.
func (m *Memtable) IsFull() bool {
	return m.arena.Size() > m.arena.Capacity()*95/100
}

// MinSeqNum returns the minimum sequence number of entries in this memtable.
func (m *Memtable) MinSeqNum() uint64 {
	return m.minSeqNum.Load()
}

// MaxSeqNum returns the maximum sequence number of entries in this memtable.
func (m *Memtable) MaxSeqNum() uint64 {
	return m.maxSeqNum.Load()
}

// Ref increments the reference count.
func (m *Memtable) Ref() {
	m.refs.Add(1)
}

// Unref decrements the reference count. Returns true if the count reached 0.
func (m *Memtable) Unref() bool {
	return m.refs.Add(-1) == 0
}

func (m *Memtable) updateSeqRange(seqNum uint64) {
	// Update min (only if smaller or first entry).
	for {
		old := m.minSeqNum.Load()
		if old != 0 && old <= seqNum {
			break
		}
		if m.minSeqNum.CompareAndSwap(old, seqNum) {
			break
		}
	}
	// Update max (only if larger).
	for {
		old := m.maxSeqNum.Load()
		if old >= seqNum {
			break
		}
		if m.maxSeqNum.CompareAndSwap(old, seqNum) {
			break
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Iterator wraps the skiplist iterator with internal key awareness.
type Iterator struct {
	skl *skiplist.Iterator
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.skl.Valid()
}

// First positions at the first entry.
func (it *Iterator) First() bool {
	return it.skl.First()
}

// Next advances to the next entry.
func (it *Iterator) Next() bool {
	return it.skl.Next()
}

// SeekGE positions at the first entry with internal key >= target.
func (it *Iterator) SeekGE(target []byte) bool {
	return it.skl.SeekGE(target)
}

// InternalKey returns the full internal key (userKey + trailer).
func (it *Iterator) InternalKey() []byte {
	return it.skl.Key()
}

// UserKey returns just the user key portion.
func (it *Iterator) UserKey() []byte {
	return UserKey(it.skl.Key())
}

// Value returns the value.
func (it *Iterator) Value() []byte {
	return it.skl.Value()
}

// SeqNum returns the sequence number from the current internal key.
func (it *Iterator) SeqNum() uint64 {
	_, seqNum, _ := ParseInternalKey(it.skl.Key())
	return seqNum
}

// Kind returns the operation kind from the current internal key.
func (it *Iterator) Kind() Kind {
	_, _, kind := ParseInternalKey(it.skl.Key())
	return kind
}
