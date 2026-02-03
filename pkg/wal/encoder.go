package wal

import (
	"encoding/binary"
	"io"
	"sync"
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

// Encoder writes records to the WAL with buffering and 8-byte alignment.
type Encoder struct {
	mu sync.Mutex
	pw *PageWriter

	buf       []byte // Pre-allocated buffer for marshaling (1MB)
	uint64buf []byte // Reusable 8-byte buffer for length field
	padBuf    []byte // Reusable padding buffer (max 7 bytes)

	offset int64 // Current offset in the WAL
}

// NewEncoder creates a new encoder writing to w.
// pageOffset is the current file offset (for page alignment).
// bufferSize is the PageWriter buffer size (0 = default 128KB).
func NewEncoder(w io.Writer, pageOffset int, bufferSize int) *Encoder {
	return &Encoder{
		pw:        NewPageWriter(w, walPageBytes, pageOffset, bufferSize),
		buf:       make([]byte, defaultEncoderBufSize),
		uint64buf: make([]byte, 8),
		padBuf:    make([]byte, 8), // Max 7 bytes padding + 1 extra
		offset:    int64(pageOffset),
	}
}

// Encode writes a record to the WAL.
// It computes the CRC, marshals the record, and writes it with 8-byte alignment.
// Returns the offset where the record was written.
func (e *Encoder) Encode(rec *Record) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Compute CRC
	rec.Crc = ComputeCRC(rec.Data)

	// Marshal into pre-allocated buffer if it fits
	var data []byte
	var err error
	size := rec.Size()
	if size <= len(e.buf) {
		n, err := rec.MarshalTo(e.buf)
		if err != nil {
			return 0, err
		}
		data = e.buf[:n]
	} else {
		// Fallback for huge records
		data, err = rec.Marshal()
		if err != nil {
			return 0, err
		}
	}

	// Calculate padding for 8-byte alignment
	lenField, padBytes := encodeFrameSize(len(data))

	// Remember offset before writing
	startOffset := e.offset

	// Write length field
	binary.LittleEndian.PutUint64(e.uint64buf, lenField)
	if _, err := e.pw.Write(e.uint64buf); err != nil {
		return 0, err
	}

	// Write data
	if _, err := e.pw.Write(data); err != nil {
		return 0, err
	}

	// Write padding
	if padBytes > 0 {
		if _, err := e.pw.Write(e.padBuf[:padBytes]); err != nil {
			return 0, err
		}
	}

	// Update offset
	e.offset += frameSizeBytes + int64(len(data)) + int64(padBytes)

	return startOffset, nil
}

// encodeFrameSize computes the length field and padding bytes.
// The lower 56 bits contain the data length.
// If padding is needed, the upper byte is 0x80 | padBytes.
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// Force 8-byte alignment so length never gets a torn write
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
