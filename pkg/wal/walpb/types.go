// Package walpb provides WAL-level record type constants and marshaling helpers.
//
// Record types are WAL-level concerns: CRC chain checkpoints, snapshot markers,
// and opaque data. Application-level distinctions (schema vs resource) are
// above this layer.
package walpb

import (
	walv1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/wal/v1"
	"google.golang.org/protobuf/proto"
)

// WAL-level record types. These are the only types the WAL understands.
// Application data is always wrapped as DataType with opaque bytes.
const (
	CrcType      int64 = 1 // CRC chain checkpoint at segment boundaries
	SnapshotType int64 = 2 // Snapshot marker (metadata-only, points to external state)
	DataType     int64 = 3 // Opaque application data
)

// Type aliases for generated protobuf types.
type (
	Record   = walv1.Record
	Snapshot = walv1.Snapshot
)

// MarshalAppend marshals rec into buf, reusing buf's capacity to avoid
// allocation when possible. Returns the marshaled bytes.
func MarshalAppend(buf []byte, rec *Record) ([]byte, error) {
	return proto.MarshalOptions{}.MarshalAppend(buf, rec)
}
