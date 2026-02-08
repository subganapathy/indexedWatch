package sstable

import "encoding/binary"

// blockWriter accumulates key-value entries with prefix compression.
//
// Block format:
//
//	[entry 0]
//	[entry 1]
//	...
//	[entry N-1]
//	[restart_offset_0: uint32]
//	[restart_offset_1: uint32]
//	...
//	[restart_offset_P-1: uint32]
//	[num_restarts: uint32]
//
// Each entry:
//
//	[shared_len: uvarint]    // prefix shared with previous key
//	[unshared_len: uvarint]  // length of unique suffix
//	[value_len: uvarint]     // length of value
//	[key_suffix: bytes]      // unshared_len bytes
//	[value: bytes]           // value_len bytes
//
// At restart points (every RestartInterval entries), shared_len = 0.
type blockWriter struct {
	buf             []byte
	restarts        []uint32
	nEntries        int
	restartInterval int
	prevKey         []byte
	tmp             [30]byte // varint scratch
}

func newBlockWriter(restartInterval int) *blockWriter {
	w := &blockWriter{
		restartInterval: restartInterval,
	}
	w.reset()
	return w
}

func (w *blockWriter) reset() {
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	w.nEntries = 0
	w.prevKey = w.prevKey[:0]
}

// add appends a key-value pair to the block.
// Keys must be added in sorted order.
func (w *blockWriter) add(key, value []byte) {
	var shared int
	if w.nEntries%w.restartInterval == 0 {
		// Restart point: store full key, record offset.
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	} else {
		// Compute shared prefix with previous key.
		shared = sharedPrefix(w.prevKey, key)
	}

	unshared := len(key) - shared

	// Encode the three varints.
	n := binary.PutUvarint(w.tmp[:], uint64(shared))
	n += binary.PutUvarint(w.tmp[n:], uint64(unshared))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(value)))
	w.buf = append(w.buf, w.tmp[:n]...)

	// Append key suffix and value.
	w.buf = append(w.buf, key[shared:]...)
	w.buf = append(w.buf, value...)

	// Save key for next prefix compression.
	w.prevKey = append(w.prevKey[:0], key...)
	w.nEntries++
}

// finish completes the block and returns its contents.
// The returned slice is valid until the next reset.
func (w *blockWriter) finish() []byte {
	// Append restart offsets.
	for _, r := range w.restarts {
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], r)
		w.buf = append(w.buf, tmp[:]...)
	}
	// Append num_restarts.
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp[:]...)
	return w.buf
}

// estimatedSize returns the approximate size of the finished block.
func (w *blockWriter) estimatedSize() int {
	return len(w.buf) + len(w.restarts)*4 + 4
}

func sharedPrefix(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	// Fast path: compare 8-byte chunks.
	i := 0
	for i+8 <= n {
		if a[i] != b[i] || a[i+1] != b[i+1] || a[i+2] != b[i+2] || a[i+3] != b[i+3] ||
			a[i+4] != b[i+4] || a[i+5] != b[i+5] || a[i+6] != b[i+6] || a[i+7] != b[i+7] {
			break
		}
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// blockIterator iterates over entries in a finished block.
// It supports seeking via binary search over restart points.
type blockIterator struct {
	data        []byte // block data (without trailer)
	restarts    int    // byte offset of restarts array
	numRestarts int    // number of restart points
	offset      int    // current position in data (next entry to decode)
	key         []byte // current full key (assembled)
	val         []byte // current value slice
	valid       bool   // true after a successful next() call
	err         error
}

func newBlockIterator(data []byte) *blockIterator {
	if len(data) < 4 {
		return &blockIterator{}
	}
	numRestarts := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartsOffset := len(data) - 4 - numRestarts*4
	return &blockIterator{
		data:        data,
		restarts:    restartsOffset,
		numRestarts: numRestarts,
		offset:      restartsOffset, // positioned past end initially
	}
}

// SeekGE positions at the first key >= target.
func (it *blockIterator) SeekGE(target []byte) bool {
	// Binary search over restart points to find the last restart <= target.
	lo, hi := 0, it.numRestarts
	for lo < hi {
		mid := lo + (hi-lo)/2
		restartOffset := it.getRestart(mid)
		key := it.decodeKeyAt(restartOffset)
		if compareBytes(key, target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// Start from the previous restart point (or the first one).
	startRestart := lo - 1
	if startRestart < 0 {
		startRestart = 0
	}
	it.offset = it.getRestart(startRestart)
	it.key = it.key[:0]

	// Linear scan to find the first key >= target.
	for it.next() {
		if compareBytes(it.key, target) >= 0 {
			return true
		}
	}
	it.valid = false
	return false
}

// First positions at the first entry.
func (it *blockIterator) First() bool {
	it.offset = 0
	it.key = it.key[:0]
	return it.next()
}

// Next advances to the next entry.
func (it *blockIterator) Next() bool {
	return it.next()
}

// Valid returns whether the iterator is positioned at a valid entry.
func (it *blockIterator) Valid() bool {
	return it.valid && it.err == nil
}

// Key returns the current key.
func (it *blockIterator) Key() []byte { return it.key }

// Value returns the current value.
func (it *blockIterator) Value() []byte { return it.val }

// Error returns any error encountered during iteration.
func (it *blockIterator) Error() error { return it.err }

func (it *blockIterator) next() bool {
	if it.offset >= it.restarts {
		it.valid = false
		return false
	}

	// Decode shared, unshared, value_len.
	shared, n := binary.Uvarint(it.data[it.offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.offset += n

	unshared, n := binary.Uvarint(it.data[it.offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.offset += n

	valueLen, n := binary.Uvarint(it.data[it.offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.offset += n

	// Reconstruct key with prefix decompression.
	it.key = append(it.key[:int(shared)], it.data[it.offset:it.offset+int(unshared)]...)
	it.offset += int(unshared)

	it.val = it.data[it.offset : it.offset+int(valueLen)]
	it.offset += int(valueLen)

	it.valid = true
	return true
}

func (it *blockIterator) getRestart(i int) int {
	return int(binary.LittleEndian.Uint32(it.data[it.restarts+i*4:]))
}

// decodeKeyAt decodes just the key at a restart point offset (shared must be 0).
func (it *blockIterator) decodeKeyAt(offset int) []byte {
	_, n := binary.Uvarint(it.data[offset:]) // shared (always 0)
	offset += n
	unshared, n := binary.Uvarint(it.data[offset:])
	offset += n
	_, n = binary.Uvarint(it.data[offset:]) // value_len
	offset += n
	return it.data[offset : offset+int(unshared)]
}

func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
