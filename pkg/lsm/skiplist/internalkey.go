package skiplist

// InternalKeyKind describes the type of operation an internal key represents.
type InternalKeyKind uint8

const (
	InternalKeyKindSet    InternalKeyKind = 1
	InternalKeyKindDelete InternalKeyKind = 2
)

// Trailer packs a sequence number and key kind into a uint64.
// Layout: [56-bit seqNum | 8-bit kind]
//
// Ordering: trailers are compared in reverse — higher trailers sort first.
// This means newer sequence numbers (larger) appear before older ones during
// iteration, which is the correct MVCC ordering.
//
// Pebble reference: internal/base/internal.go
type Trailer uint64

// MakeTrailer constructs a trailer from a sequence number and key kind.
func MakeTrailer(seqNum uint64, kind InternalKeyKind) Trailer {
	return Trailer(seqNum<<8) | Trailer(kind)
}

// SeqNum extracts the sequence number from the trailer.
func (t Trailer) SeqNum() uint64 {
	return uint64(t >> 8)
}

// Kind extracts the key kind from the trailer.
func (t Trailer) Kind() InternalKeyKind {
	return InternalKeyKind(t & 0xff)
}
