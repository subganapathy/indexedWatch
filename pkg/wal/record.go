// Package wal implements a generic Write-Ahead Log with configurable
// rotation and sync policies. It follows etcd's WAL patterns:
// - Buffered writes (no fsync per write)
// - Pre-allocated buffers to avoid allocations
// - 8-byte aligned wire format to prevent torn writes
// - CRC verification for data integrity
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// ErrCRCMismatch is returned when a record's CRC doesn't match the computed CRC.
var ErrCRCMismatch = errors.New("wal: crc mismatch")

// crcTable is the Castagnoli CRC32 table (same as etcd).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is a generic WAL entry envelope.
// The wire format is:
//
//	Type: int64 (8 bytes, little-endian)
//	Crc:  uint32 (4 bytes, little-endian)
//	Len:  uint32 (4 bytes, little-endian) - length of Data
//	Data: []byte (Len bytes)
//
// Total fixed header: 16 bytes + variable data.
type Record struct {
	Type int64  // Application-defined type (e.g., SchemaRegister=1, SchemaUpdate=2)
	Data []byte // Opaque payload (protobuf, JSON, etc.)
	Crc  uint32 // CRC32-C of Data
}

const recordHeaderSize = 16 // Type(8) + Crc(4) + Len(4)

// Size returns the marshaled size of the record.
func (r *Record) Size() int {
	return recordHeaderSize + len(r.Data)
}

// MarshalTo marshals the record into the provided buffer.
// The buffer must be at least r.Size() bytes.
// Returns the number of bytes written.
func (r *Record) MarshalTo(buf []byte) (int, error) {
	if len(buf) < r.Size() {
		return 0, fmt.Errorf("wal: buffer too small: need %d, got %d", r.Size(), len(buf))
	}

	binary.LittleEndian.PutUint64(buf[0:8], uint64(r.Type))
	binary.LittleEndian.PutUint32(buf[8:12], r.Crc)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(r.Data)))
	copy(buf[16:], r.Data)

	return r.Size(), nil
}

// Marshal allocates a new buffer and marshals the record into it.
func (r *Record) Marshal() ([]byte, error) {
	buf := make([]byte, r.Size())
	_, err := r.MarshalTo(buf)
	return buf, err
}

// Unmarshal unmarshals a record from the provided buffer.
func (r *Record) Unmarshal(data []byte) error {
	if len(data) < recordHeaderSize {
		return fmt.Errorf("wal: record too short: need at least %d bytes, got %d", recordHeaderSize, len(data))
	}

	r.Type = int64(binary.LittleEndian.Uint64(data[0:8]))
	r.Crc = binary.LittleEndian.Uint32(data[8:12])
	dataLen := binary.LittleEndian.Uint32(data[12:16])

	if len(data) < recordHeaderSize+int(dataLen) {
		return fmt.Errorf("wal: record data truncated: expected %d data bytes, got %d", dataLen, len(data)-recordHeaderSize)
	}

	r.Data = make([]byte, dataLen)
	copy(r.Data, data[16:16+dataLen])

	return nil
}

// Reset resets the record to its zero value.
func (r *Record) Reset() {
	r.Type = 0
	r.Crc = 0
	r.Data = nil
}

// Validate checks if the record's CRC matches the expected CRC.
func (r *Record) Validate(expectedCrc uint32) error {
	if r.Crc == expectedCrc {
		return nil
	}
	return fmt.Errorf("%w: expected %x, got %x", ErrCRCMismatch, expectedCrc, r.Crc)
}

// ComputeCRC computes the CRC32-C of the record's data.
func ComputeCRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}
