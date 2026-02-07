package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/subganapathy/indexedwatch/pkg/crc"
	"github.com/subganapathy/indexedwatch/pkg/wal/walpb"
	"google.golang.org/protobuf/proto"
)

// Decoder reads and decodes records from a WAL file.
// It maintains a running CRC chain for validation, matching the encoder's
// chaining behavior.
type Decoder struct {
	r            *bufio.Reader
	crc          hash.Hash32 // Running CRC chain for validation
	lastValidOff int64
	uint64buf    []byte // Reusable buffer for reading length field
}

// NewDecoder creates a new decoder reading from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:         bufio.NewReaderSize(r, 64*1024), // 64KB read buffer
		crc:       crc.New(0, crcTable),
		uint64buf: make([]byte, 8),
	}
}

// Decode reads the next record from the WAL.
// Returns io.EOF when there are no more records.
// Returns io.ErrUnexpectedEOF for partial/torn writes (recoverable).
// Returns ErrCorrupted or ErrCRCMismatch for data corruption (not recoverable).
func (d *Decoder) Decode(rec *walpb.Record) error {
	rec.Reset()

	// Read length field (8 bytes).
	_, err := io.ReadFull(d.r, d.uint64buf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return err
	}

	lenField := binary.LittleEndian.Uint64(d.uint64buf)

	// Check for zero length (end of valid data / preallocated space).
	if lenField == 0 {
		return io.EOF
	}

	// Decode frame size.
	recBytes, padBytes := decodeFrameSize(int64(lenField))

	// Sanity check: record size should be reasonable.
	const maxRecordSize = 64 * 1024 * 1024 // 64MB max
	if recBytes > maxRecordSize {
		return fmt.Errorf("%w: record size %d exceeds maximum %d", ErrCorrupted, recBytes, maxRecordSize)
	}

	// Read data + padding.
	data := make([]byte, recBytes+padBytes)
	_, err = io.ReadFull(d.r, data)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// Unmarshal protobuf record (excluding padding).
	if err := proto.Unmarshal(data[:recBytes], rec); err != nil {
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return fmt.Errorf("%w: %v", ErrCorrupted, err)
	}

	// Validate CRC chain.
	if rec.Type == walpb.CrcType {
		// CrcType records reset the chain. The stored Crc is the previous
		// encoder's final CRC value, used to seed the new chain.
		d.crc = crc.New(rec.Crc, crcTable)
	} else {
		// For all other records: feed data into the chain, then compare.
		d.crc.Write(rec.Data)
		if d.crc.Sum32() != rec.Crc {
			if d.isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
			return fmt.Errorf("%w: stored %08x, computed %08x", ErrCRCMismatch, rec.Crc, d.crc.Sum32())
		}
	}

	// Update last valid offset.
	d.lastValidOff += frameSizeBytes + recBytes + padBytes

	return nil
}

// decodeFrameSize extracts the record size and padding from the length field.
func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	if lenField < 0 {
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry checks if the data looks like a torn write.
// A torn write typically results in sectors filled with zeros.
func (d *Decoder) isTornEntry(data []byte) bool {
	fileOff := d.lastValidOff + frameSizeBytes
	curOff := 0

	for curOff < len(data) {
		chunkLen := min(int(minSectorSize-(fileOff%minSectorSize)), len(data)-curOff)

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

// LastCRC returns the running CRC value after all decoded records.
// Used to seed the encoder for the read→write transition.
func (d *Decoder) LastCRC() uint32 {
	return d.crc.Sum32()
}
