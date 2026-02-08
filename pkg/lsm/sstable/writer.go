package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// WriterOptions configures the SSTable writer.
type WriterOptions struct {
	BlockSize       int
	RestartInterval int
	BloomBitsPerKey int
}

// DefaultWriterOptions returns sensible defaults.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		BlockSize:       DefaultBlockSize,
		RestartInterval: DefaultRestartInterval,
		BloomBitsPerKey: DefaultBloomBitsPerKey,
	}
}

// Writer creates an SSTable file.
// Keys must be added in strictly sorted order.
type Writer struct {
	w      io.Writer
	offset uint64

	dataBlock   *blockWriter
	indexBlock  *blockWriter
	bloom       *bloomFilterWriter
	opts        WriterOptions
	keyCount    uint64
	smallestKey []byte
	largestKey  []byte
	prevKey     []byte

	// Accumulated block handles for writing index entries.
	pendingHandle *BlockHandle // set after flushing a data block
	pendingKey    []byte       // last key of the flushed block

	dataBlockCount int
	filterHandle   BlockHandle
	indexHandle    BlockHandle
	propsHandle    BlockHandle
}

// NewWriter creates a new SSTable writer that writes to the given io.Writer.
func NewWriter(w io.Writer, opts WriterOptions) *Writer {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	if opts.RestartInterval <= 0 {
		opts.RestartInterval = DefaultRestartInterval
	}
	if opts.BloomBitsPerKey <= 0 {
		opts.BloomBitsPerKey = DefaultBloomBitsPerKey
	}
	return &Writer{
		w:          w,
		dataBlock:  newBlockWriter(opts.RestartInterval),
		indexBlock:  newBlockWriter(1), // index block: every entry is a restart point
		bloom:      newBloomFilterWriter(opts.BloomBitsPerKey),
		opts:       opts,
	}
}

// Add adds a key-value pair to the SSTable.
// Keys must be added in strictly increasing lexicographic order.
func (w *Writer) Add(key, value []byte) error {
	if w.prevKey != nil && compareBytes(key, w.prevKey) <= 0 {
		return fmt.Errorf("sstable: keys must be added in order: %q <= %q", key, w.prevKey)
	}

	// If there's a pending index entry from a previous data block flush,
	// add it now using a separator between the previous block's last key
	// and this new key.
	if w.pendingHandle != nil {
		sep := shortSeparator(w.pendingKey, key)
		w.addIndexEntry(sep, *w.pendingHandle)
		w.pendingHandle = nil
	}

	// Track key range.
	if w.keyCount == 0 {
		w.smallestKey = append(w.smallestKey[:0], key...)
	}
	w.largestKey = append(w.largestKey[:0], key...)

	// Add to data block and bloom filter.
	w.dataBlock.add(key, value)
	w.bloom.addKey(key)
	w.keyCount++

	// Flush data block if it's big enough.
	if w.dataBlock.estimatedSize() >= w.opts.BlockSize {
		if err := w.flushDataBlock(); err != nil {
			return err
		}
	}

	w.prevKey = append(w.prevKey[:0], key...)
	return nil
}

// Close finishes writing the SSTable and returns metadata.
func (w *Writer) Close() (*TableMeta, error) {
	// Flush any remaining data block.
	if w.dataBlock.nEntries > 0 {
		if err := w.flushDataBlock(); err != nil {
			return nil, err
		}
	}

	// Add final index entry for the last data block.
	if w.pendingHandle != nil {
		// Use a short successor for the last block (no next key to compare against).
		sep := shortSuccessor(w.pendingKey)
		w.addIndexEntry(sep, *w.pendingHandle)
		w.pendingHandle = nil
	}

	// Write filter block.
	filterData := w.bloom.finish()
	if filterData != nil {
		var err error
		w.filterHandle, err = w.writeBlock(filterData)
		if err != nil {
			return nil, err
		}
	}

	// Write index block.
	indexData := w.indexBlock.finish()
	var err error
	w.indexHandle, err = w.writeBlock(indexData)
	if err != nil {
		return nil, err
	}

	// Write properties block.
	propsBlock := newBlockWriter(1)
	w.addProperty(propsBlock, "indexedwatch.key_count", fmt.Sprintf("%d", w.keyCount))
	w.addProperty(propsBlock, "indexedwatch.data_blocks", fmt.Sprintf("%d", w.dataBlockCount))
	propsData := propsBlock.finish()
	w.propsHandle, err = w.writeBlock(propsData)
	if err != nil {
		return nil, err
	}

	// Write metaindex block.
	metaBlock := newBlockWriter(1)
	if filterData != nil {
		metaBlock.add([]byte("indexedwatch.bloom_filter"), encodeHandle(w.filterHandle))
	}
	metaBlock.add([]byte("indexedwatch.properties"), encodeHandle(w.propsHandle))
	metaData := metaBlock.finish()
	metaHandle, err := w.writeBlock(metaData)
	if err != nil {
		return nil, err
	}

	// Write footer.
	if err := w.writeFooter(metaHandle, w.indexHandle); err != nil {
		return nil, err
	}

	return &TableMeta{
		Size:           w.offset,
		KeyCount:       w.keyCount,
		SmallestKey:    w.smallestKey,
		LargestKey:     w.largestKey,
		DataBlockCount: w.dataBlockCount,
	}, nil
}

func (w *Writer) flushDataBlock() error {
	data := w.dataBlock.finish()
	handle, err := w.writeBlock(data)
	if err != nil {
		return err
	}

	w.pendingHandle = &handle
	w.pendingKey = append(w.pendingKey[:0], w.dataBlock.prevKey...)
	w.dataBlock.reset()
	w.dataBlockCount++

	return nil
}

func (w *Writer) addIndexEntry(separator []byte, handle BlockHandle) {
	w.indexBlock.add(separator, encodeHandle(handle))
}

func (w *Writer) addProperty(block *blockWriter, key, value string) {
	block.add([]byte(key), []byte(value))
}

// writeBlock writes a block with its 5-byte trailer (type + checksum).
func (w *Writer) writeBlock(data []byte) (BlockHandle, error) {
	handle := BlockHandle{Offset: w.offset, Length: uint64(len(data))}

	// Write block data.
	if _, err := w.w.Write(data); err != nil {
		return BlockHandle{}, err
	}

	// Write trailer: compression type + checksum.
	var trailer [blockTrailerSize]byte
	trailer[0] = compressionNone
	checksum := blockChecksum(data, compressionNone)
	binary.LittleEndian.PutUint32(trailer[1:], checksum)
	if _, err := w.w.Write(trailer[:]); err != nil {
		return BlockHandle{}, err
	}

	w.offset += uint64(len(data)) + blockTrailerSize
	return handle, nil
}

func (w *Writer) writeFooter(metaindexHandle, indexHandle BlockHandle) error {
	var footer [footerSize]byte
	n := metaindexHandle.encode(footer[:])
	n += indexHandle.encode(footer[n:])
	// Remaining bytes are zero-padding.
	// Write magic at the end.
	copy(footer[footerSize-8:], magic[:])

	_, err := w.w.Write(footer[:])
	w.offset += footerSize
	return err
}

// encodeHandle encodes a BlockHandle to bytes.
func encodeHandle(h BlockHandle) []byte {
	buf := make([]byte, h.encodedLen())
	h.encode(buf)
	return buf
}

// shortSeparator returns a short key that is >= a and < b.
// Used for index block entries to minimize index size.
func shortSeparator(a, b []byte) []byte {
	// Find the shared prefix.
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	shared := 0
	for shared < n && a[shared] == b[shared] {
		shared++
	}

	// If a is a prefix of b, or they're equal, just use a.
	if shared == len(a) {
		return append([]byte{}, a...)
	}

	// If we can increment the byte at the shared position and stay < b,
	// use the shortened key.
	if a[shared] < 0xFF && a[shared]+1 < b[shared] {
		sep := make([]byte, shared+1)
		copy(sep, a[:shared])
		sep[shared] = a[shared] + 1
		return sep
	}

	return append([]byte{}, a...)
}

// shortSuccessor returns a short key >= a (for the last index entry).
func shortSuccessor(a []byte) []byte {
	for i := len(a) - 1; i >= 0; i-- {
		if a[i] < 0xFF {
			s := make([]byte, i+1)
			copy(s, a[:i])
			s[i] = a[i] + 1
			return s
		}
	}
	return append([]byte{}, a...)
}

// CreateWriter is a convenience that creates an SSTable file on disk.
func CreateWriter(path string, opts WriterOptions) (*Writer, *os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return NewWriter(f, opts), f, nil
}
