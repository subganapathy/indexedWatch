package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// recordVersion is the binary encoding version for StoredRecord.
	recordVersion = 2

	// recordHeaderSize is the fixed-size header:
	//   1 byte:  version
	//   1 byte:  flags (bit 0 = isDeleted)
	//   8 bytes: createSeqNum (big-endian)
	//   8 bytes: updateSeqNum (big-endian)
	recordHeaderSize = 1 + 1 + 8 + 8
)

// StoredRecord is the value format stored in the Primary LSM.
//
// The LSM key is the primary key. The LSM value is the binary-encoded
// StoredRecord. Binary encoding is used (rather than protobuf) because
// this is on the hot read/write path.
type StoredRecord struct {
	// Data is the resource payload (opaque bytes, typically JSON).
	// Nil for deleted records.
	Data []byte

	// CreateSeqNum is the storage sequence number when the record was first created.
	CreateSeqNum uint64

	// UpdateSeqNum is the storage sequence number of the last mutation.
	// Used for OCC: clients pass this as expectedSeqNum on subsequent writes.
	UpdateSeqNum uint64

	// IsDeleted marks the record as logically deleted.
	// Deleted records are retained (with metadata) so secondary index
	// builders can observe the deletion and clean up SK entries.
	IsDeleted bool
}

// Marshal encodes the StoredRecord into a binary format suitable for LSM storage.
//
// Format:
//
//	[1] version
//	[1] flags
//	[8] createSeqNum (big-endian)
//	[8] updateSeqNum (big-endian)
//	[M] data bytes
func (r *StoredRecord) Marshal() []byte {
	size := recordHeaderSize + len(r.Data)
	buf := make([]byte, size)

	buf[0] = recordVersion
	if r.IsDeleted {
		buf[1] = 1
	}
	binary.BigEndian.PutUint64(buf[2:], r.CreateSeqNum)
	binary.BigEndian.PutUint64(buf[10:], r.UpdateSeqNum)
	copy(buf[recordHeaderSize:], r.Data)

	return buf
}

// UnmarshalStoredRecord decodes a binary-encoded StoredRecord.
// Returns an error if the data is too short or has an unknown version.
func UnmarshalStoredRecord(buf []byte) (*StoredRecord, error) {
	if len(buf) < recordHeaderSize {
		return nil, errors.New("storage: record too short")
	}
	if buf[0] != recordVersion {
		return nil, fmt.Errorf("storage: unknown record version %d", buf[0])
	}

	r := &StoredRecord{
		IsDeleted:    buf[1]&1 != 0,
		CreateSeqNum: binary.BigEndian.Uint64(buf[2:]),
		UpdateSeqNum: binary.BigEndian.Uint64(buf[10:]),
	}

	if recordHeaderSize < len(buf) {
		r.Data = make([]byte, len(buf)-recordHeaderSize)
		copy(r.Data, buf[recordHeaderSize:])
	}

	return r, nil
}
