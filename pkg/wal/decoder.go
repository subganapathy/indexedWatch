package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ErrCorrupted is returned when WAL data corruption is detected.
var ErrCorrupted = errors.New("wal: data corrupted")

// Decoder reads and decodes records from a WAL file.
type Decoder struct {
	r            *bufio.Reader
	lastValidOff int64
	uint64buf    []byte // Reusable buffer for reading length field
}

// NewDecoder creates a new decoder reading from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:         bufio.NewReaderSize(r, 64*1024), // 64KB read buffer
		uint64buf: make([]byte, 8),
	}
}

// Decode reads the next record from the WAL.
// Returns io.EOF when there are no more records.
// Returns io.ErrUnexpectedEOF for partial/torn writes (recoverable).
// Returns ErrCorrupted or ErrCRCMismatch for data corruption (not recoverable).
func (d *Decoder) Decode(rec *Record) error {
	rec.Reset()

	// Read length field (8 bytes)
	_, err := io.ReadFull(d.r, d.uint64buf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return err
	}

	lenField := binary.LittleEndian.Uint64(d.uint64buf)

	// Check for zero length (end of valid data / preallocated space)
	if lenField == 0 {
		return io.EOF
	}

	// Decode frame size
	recBytes, padBytes := decodeFrameSize(int64(lenField))

	// Sanity check: record size should be reasonable
	const maxRecordSize = 64 * 1024 * 1024 // 64MB max
	if recBytes > maxRecordSize {
		return fmt.Errorf("%w: record size %d exceeds maximum %d", ErrCorrupted, recBytes, maxRecordSize)
	}

	// Read data + padding
	data := make([]byte, recBytes+padBytes)
	_, err = io.ReadFull(d.r, data)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Partial read = torn write
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// Unmarshal record (excluding padding)
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		// Check if this looks like a torn write (zeros)
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return fmt.Errorf("%w: %v", ErrCorrupted, err)
	}

	// Verify CRC
	expectedCrc := ComputeCRC(rec.Data)
	if err := rec.Validate(expectedCrc); err != nil {
		// Check if this looks like a torn write
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// Update last valid offset
	d.lastValidOff += frameSizeBytes + recBytes + padBytes

	return nil
}

// decodeFrameSize extracts the record size and padding from the length field.
func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// The record size is stored in the lower 56 bits
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// Non-zero padding is indicated by set MSB (negative length)
	if lenField < 0 {
		// Padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry checks if the data looks like a torn write.
// A torn write typically results in sectors filled with zeros.
func (d *Decoder) isTornEntry(data []byte) bool {
	// Split data on sector boundaries and check for all-zero sectors
	fileOff := d.lastValidOff + frameSizeBytes
	curOff := 0

	for curOff < len(data) {
		chunkLen := min(int(minSectorSize-(fileOff%minSectorSize)), len(data)-curOff)

		// Check if this chunk is all zeros
		isZero := true
		for i := curOff; i < curOff+chunkLen; i++ {
			if data[i] != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}

		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	return false
}

// LastOffset returns the file offset after the last successfully decoded record.
func (d *Decoder) LastOffset() int64 {
	return d.lastValidOff
}
