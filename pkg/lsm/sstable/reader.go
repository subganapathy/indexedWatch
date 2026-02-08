package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	// ErrNotFound indicates a key was not found in the SSTable.
	ErrNotFound = errors.New("sstable: key not found")
	// ErrCorrupted indicates the SSTable file is corrupted.
	ErrCorrupted = errors.New("sstable: corrupted")
)

// Reader reads an SSTable file. The index block and bloom filter are loaded
// into memory on open. Data blocks are loaded on demand, optionally through
// a shared BlockCache.
type Reader struct {
	r      io.ReaderAt
	size   int64
	fileID uint64

	indexBlock  []byte       // raw index block data (no trailer)
	filter     *bloomFilter // bloom filter (nil if none)
	cache      *BlockCache  // shared block cache (nil for no caching)

	metaindexHandle BlockHandle
	indexHandle     BlockHandle
}

// ReaderOptions configures the SSTable reader.
type ReaderOptions struct {
	// Cache is an optional shared block cache. If nil, data blocks are
	// read from disk on every access.
	Cache *BlockCache
	// FileID uniquely identifies this SSTable for cache keying.
	FileID uint64
}

// NewReader opens an SSTable from an io.ReaderAt.
func NewReader(r io.ReaderAt, size int64, opts ReaderOptions) (*Reader, error) {
	if size < footerSize {
		return nil, fmt.Errorf("%w: file too small (%d bytes)", ErrCorrupted, size)
	}

	reader := &Reader{
		r:      r,
		size:   size,
		fileID: opts.FileID,
		cache:  opts.Cache,
	}

	if err := reader.readFooter(); err != nil {
		return nil, err
	}
	if err := reader.readIndex(); err != nil {
		return nil, err
	}
	if err := reader.readFilter(); err != nil {
		return nil, err
	}

	return reader, nil
}

// OpenReader opens an SSTable file from disk.
func OpenReader(path string, opts ReaderOptions) (*Reader, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	reader, err := NewReader(f, info.Size(), opts)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return reader, f, nil
}

// Get looks up a single key. Returns the value and true if found,
// or nil and false if not found. Uses bloom filter for fast rejection.
func (r *Reader) Get(key []byte) ([]byte, error) {
	// Bloom filter check.
	if r.filter != nil && !r.filter.mayContain(key) {
		return nil, ErrNotFound
	}

	// Find the data block that may contain the key via index block.
	handle, err := r.findDataBlock(key)
	if err != nil {
		return nil, err
	}

	// Read and search the data block.
	blockData, err := r.readDataBlock(handle)
	if err != nil {
		return nil, err
	}

	it := newBlockIterator(blockData)
	if it.SeekGE(key) && compareBytes(it.Key(), key) == 0 {
		// Copy value since the block data may be evicted from cache.
		val := make([]byte, len(it.Value()))
		copy(val, it.Value())
		return val, nil
	}
	return nil, ErrNotFound
}

// NewIterator returns a two-level iterator over the SSTable.
// The iterator uses the index block to locate data blocks on demand.
func (r *Reader) NewIterator() *TableIterator {
	return &TableIterator{reader: r}
}

// readFooter reads and validates the footer from the end of the file.
func (r *Reader) readFooter() error {
	var footer [footerSize]byte
	if _, err := r.r.ReadAt(footer[:], r.size-footerSize); err != nil {
		return fmt.Errorf("%w: failed to read footer: %v", ErrCorrupted, err)
	}

	// Validate magic.
	if string(footer[footerSize-8:]) != string(magic[:]) {
		return fmt.Errorf("%w: bad magic number", ErrCorrupted)
	}

	// Decode handles.
	var n int
	r.metaindexHandle, n = decodeBlockHandle(footer[:])
	r.indexHandle, _ = decodeBlockHandle(footer[n:])

	return nil
}

// readIndex loads the index block into memory.
func (r *Reader) readIndex() error {
	data, err := r.readRawBlock(r.indexHandle)
	if err != nil {
		return fmt.Errorf("sstable: failed to read index block: %w", err)
	}
	r.indexBlock = data
	return nil
}

// readFilter loads the bloom filter from the metaindex.
func (r *Reader) readFilter() error {
	metaData, err := r.readRawBlock(r.metaindexHandle)
	if err != nil {
		return fmt.Errorf("sstable: failed to read metaindex block: %w", err)
	}

	// Scan metaindex for the filter block handle.
	it := newBlockIterator(metaData)
	for it.First(); it.Valid(); it.Next() {
		if string(it.Key()) == "indexedwatch.bloom_filter" {
			filterHandle, _ := decodeBlockHandle(it.Value())
			filterData, err := r.readRawBlock(filterHandle)
			if err != nil {
				return fmt.Errorf("sstable: failed to read filter block: %w", err)
			}
			r.filter = decodeBloomFilter(filterData)
			return nil
		}
	}

	// No filter block — that's okay.
	return nil
}

// findDataBlock searches the index block for the data block that may contain key.
func (r *Reader) findDataBlock(key []byte) (BlockHandle, error) {
	it := newBlockIterator(r.indexBlock)
	if !it.SeekGE(key) {
		// Key is beyond all indexed blocks. Check the last block.
		// If the index is empty, nothing to return.
		if !it.First() {
			return BlockHandle{}, ErrNotFound
		}
		// Scan to the last entry.
		for it.Next() {
		}
		if !it.Valid() {
			return BlockHandle{}, ErrNotFound
		}
	}

	handle, _ := decodeBlockHandle(it.Value())
	return handle, nil
}

// readDataBlock reads a data block, checking the cache first.
func (r *Reader) readDataBlock(handle BlockHandle) ([]byte, error) {
	// Check cache.
	if r.cache != nil {
		if data := r.cache.Get(r.fileID, handle.Offset); data != nil {
			return data, nil
		}
	}

	data, err := r.readRawBlock(handle)
	if err != nil {
		return nil, err
	}

	// Cache the block.
	if r.cache != nil {
		r.cache.Set(r.fileID, handle.Offset, data)
	}

	return data, nil
}

// readRawBlock reads a block from disk and validates its checksum.
func (r *Reader) readRawBlock(handle BlockHandle) ([]byte, error) {
	// Read block data + trailer.
	buf := make([]byte, handle.Length+blockTrailerSize)
	if _, err := r.r.ReadAt(buf, int64(handle.Offset)); err != nil {
		return nil, err
	}

	data := buf[:handle.Length]
	blockType := buf[handle.Length]
	storedChecksum := binary.LittleEndian.Uint32(buf[handle.Length+1:])

	// Validate checksum.
	computed := blockChecksum(data, blockType)
	if computed != storedChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch (computed %d, stored %d)",
			ErrCorrupted, computed, storedChecksum)
	}

	return data, nil
}

// TableIterator is a two-level iterator over an SSTable.
// It uses the index block to locate data blocks and iterates within each block.
type TableIterator struct {
	reader    *Reader
	indexIter *blockIterator // iterates over index block entries
	dataIter  *blockIterator // iterates within current data block
	err       error
}

// SeekGE positions at the first key >= target.
func (it *TableIterator) SeekGE(target []byte) bool {
	it.err = nil
	it.indexIter = newBlockIterator(it.reader.indexBlock)

	if !it.indexIter.SeekGE(target) {
		return false
	}

	if err := it.loadDataBlock(); err != nil {
		it.err = err
		return false
	}

	if it.dataIter.SeekGE(target) {
		return true
	}

	// Key might be in the next data block.
	return it.nextBlock()
}

// First positions at the first key in the SSTable.
func (it *TableIterator) First() bool {
	it.err = nil
	it.indexIter = newBlockIterator(it.reader.indexBlock)

	if !it.indexIter.First() {
		return false
	}

	if err := it.loadDataBlock(); err != nil {
		it.err = err
		return false
	}

	return it.dataIter.First()
}

// Next advances to the next key.
func (it *TableIterator) Next() bool {
	if it.dataIter == nil {
		return false
	}

	if it.dataIter.Next() {
		return true
	}

	return it.nextBlock()
}

// Valid returns whether the iterator is positioned at a valid entry.
func (it *TableIterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid() && it.err == nil
}

// Key returns the current key.
func (it *TableIterator) Key() []byte {
	return it.dataIter.Key()
}

// Value returns the current value.
func (it *TableIterator) Value() []byte {
	return it.dataIter.Value()
}

// Error returns any error encountered.
func (it *TableIterator) Error() error {
	return it.err
}

func (it *TableIterator) nextBlock() bool {
	if !it.indexIter.Next() {
		it.dataIter = nil
		return false
	}

	if err := it.loadDataBlock(); err != nil {
		it.err = err
		return false
	}

	return it.dataIter.First()
}

func (it *TableIterator) loadDataBlock() error {
	handle, _ := decodeBlockHandle(it.indexIter.Value())
	data, err := it.reader.readDataBlock(handle)
	if err != nil {
		return err
	}
	it.dataIter = newBlockIterator(data)
	return nil
}
