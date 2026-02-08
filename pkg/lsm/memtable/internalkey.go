// Package memtable wraps the skiplist with WAL sequence number tracking and
// internal key encoding for MVCC.
//
// Internal key format: userKey + 8-byte trailer
// The trailer encodes both the sequence number and the key kind:
//   trailer = (seqNum << 8) | kind
//
// When sorted lexicographically, keys with the same userKey are ordered
// by descending seqNum (newest first), because the trailer bytes are
// appended in big-endian and we compare as raw bytes.
//
// Pebble reference: internal/base/internal.go (InternalKey trailer)
package memtable

import "encoding/binary"

// Kind describes the operation type for a key.
type Kind uint8

const (
	// KindSet represents a set (upsert) operation.
	KindSet Kind = 1
	// KindDelete represents a delete (tombstone) operation.
	KindDelete Kind = 0
)

const trailerSize = 8

// MakeInternalKey constructs an internal key from a user key, sequence number,
// and operation kind.
//
// The trailer is the bitwise complement of ((seqNum << 8) | kind), stored in
// big-endian. Inverting the entire combined value ensures that lexicographic
// byte comparison gives descending order for both seqNum and kind:
//   - Same userKey, higher seqNum → sorts first (newest wins)
//   - Same userKey+seqNum, KindSet(1) → sorts before KindDelete(0)
//
// We use descending seqNum because newer writes must shadow older ones:
// a Get should find the most recent version of a key.
func MakeInternalKey(userKey []byte, seqNum uint64, kind Kind) []byte {
	ikey := make([]byte, len(userKey)+trailerSize)
	copy(ikey, userKey)
	// Invert the combined (seqNum<<8|kind) so larger values sort first.
	trailer := ^((seqNum << 8) | uint64(kind))
	binary.BigEndian.PutUint64(ikey[len(userKey):], trailer)
	return ikey
}

// ParseInternalKey splits an internal key into its components.
func ParseInternalKey(ikey []byte) (userKey []byte, seqNum uint64, kind Kind) {
	if len(ikey) < trailerSize {
		return ikey, 0, 0
	}
	userKey = ikey[:len(ikey)-trailerSize]
	trailer := binary.BigEndian.Uint64(ikey[len(ikey)-trailerSize:])
	val := ^trailer
	kind = Kind(val & 0xFF)
	seqNum = val >> 8
	return
}

// UserKey extracts the user key from an internal key (strips the trailer).
func UserKey(ikey []byte) []byte {
	if len(ikey) < trailerSize {
		return ikey
	}
	return ikey[:len(ikey)-trailerSize]
}
