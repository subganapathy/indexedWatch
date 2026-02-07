package wal

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"sync"

	"github.com/subganapathy/indexedwatch/pkg/crc"
	"github.com/subganapathy/indexedwatch/pkg/wal/walpb"
)

const (
	// frameSizeBytes is the size of the length field (8 bytes for alignment).
	frameSizeBytes = 8
	// defaultEncoderBufSize is the pre-allocated buffer size (1MB like etcd).
	defaultEncoderBufSize = 1024 * 1024
	// minSectorSize is the minimum disk sector size for torn write detection.
	minSectorSize = 512
	// walPageBytes is the alignment for flushing records.
	walPageBytes = 8 * minSectorSize // 4KB
)

// crcTable is the Castagnoli CRC32 table.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Encoder writes records to the WAL with buffering and 8-byte alignment.
// It maintains a running CRC chain across all records: each record's CRC
// is the cumulative hash of all data written so far.
type Encoder struct {
	mu  sync.Mutex
	pw  *PageWriter
	crc hash.Hash32 // Running CRC chain

	buf       []byte // Pre-allocated buffer for protobuf marshaling (1MB)
	uint64buf []byte // Reusable 8-byte buffer for length field
	padBuf    []byte // Reusable padding buffer (max 7 bytes)

	offset int64 // Current offset in the WAL
}

// NewEncoder creates a new encoder writing to w.
// prevCrc seeds the CRC chain (0 for new WAL, decoder.LastCRC() for recovery).
// pageOffset is the current file offset (for page alignment).
// bufferSize is the PageWriter buffer size (0 = default 128KB).
func NewEncoder(w io.Writer, prevCrc uint32, pageOffset int, bufferSize int) *Encoder {
	return &Encoder{
		pw:        NewPageWriter(w, walPageBytes, pageOffset, bufferSize),
		crc:       crc.New(prevCrc, crcTable),
		buf:       make([]byte, defaultEncoderBufSize),
		uint64buf: make([]byte, 8),
		padBuf:    make([]byte, 8),
		offset:    int64(pageOffset),
	}
}

// Encode writes a record to the WAL.
// It chains the CRC (feeding rec.Data into the running hash), marshals
// via protobuf, and writes the frame with 8-byte alignment.
// Returns the offset where the record was written.
func (e *Encoder) Encode(rec *walpb.Record) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Chain CRC: feed this record's data into the running hash.
	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()

	// Marshal into pre-allocated buffer if it fits.
	var err error
	data := e.buf[:0]
	data, err = walpb.MarshalAppend(data, rec)
	if err != nil {
		return 0, err
	}

	// Calculate padding for 8-byte alignment.
	lenField, padBytes := encodeFrameSize(len(data))

	// Remember offset before writing.
	startOffset := e.offset

	// Write length field.
	binary.LittleEndian.PutUint64(e.uint64buf, lenField)
	if _, err := e.pw.Write(e.uint64buf); err != nil {
		return 0, err
	}

	// Write data.
	if _, err := e.pw.Write(data); err != nil {
		return 0, err
	}

	// Write padding.
	if padBytes > 0 {
		if _, err := e.pw.Write(e.padBuf[:padBytes]); err != nil {
			return 0, err
		}
	}

	// Update offset.
	e.offset += frameSizeBytes + int64(len(data)) + int64(padBytes)

	return startOffset, nil
}

// encodeFrameSize computes the length field and padding bytes.
// The lower 56 bits contain the data length.
// If padding is needed, the upper byte is 0x80 | padBytes.
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

// Flush flushes buffered data to the underlying writer.
func (e *Encoder) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pw.Flush()
}

// CRC returns the current cumulative CRC value.
// Used for bridging the CRC chain across segment boundaries (rotation)
// and for the read→write transition.
func (e *Encoder) CRC() uint32 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.crc.Sum32()
}

// Offset returns the current write offset.
func (e *Encoder) Offset() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.offset
}

// Buffered returns the number of bytes currently buffered.
func (e *Encoder) Buffered() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pw.Buffered()
}
