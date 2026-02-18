// Package sstable implements an on-disk sorted string table format.
//
// File layout:
//
//	[data block 0]
//	[data block 1]
//	...
//	[data block N-1]
//	[filter block]         (partitioned bloom filter)
//	[index block]          (separator keys → data block handles)
//	[properties block]     (metadata key-value pairs)
//	[metaindex block]      (meta block names → handles)
//	[footer]               (metaindex handle, index handle, magic)
//
// Each block has a 5-byte trailer: [compression type: 1 byte][checksum: 4 bytes].
//
// Pebble reference: sstable/writer.go, sstable/reader.go, sstable/block.go
package sstable

import (
	"encoding/binary"
	"hash/crc32"
)

// Magic number identifies our SSTable format.
var magic = [8]byte{'i', 'w', 's', 's', 't', 'v', '0', '1'}

const (
	// blockTrailerSize is the per-block trailer: type byte + 4-byte checksum.
	blockTrailerSize = 5

	// footerSize is the fixed size of the footer.
	// metaindexHandle (max 20) + indexHandle (max 20) + padding + magic (8).
	footerSize = 48

	// DefaultBlockSize is the target size for data blocks before flushing.
	DefaultBlockSize = 4096

	// DefaultRestartInterval is the number of keys between restart points
	// in data blocks. At each restart point, the full key is stored
	// (no prefix compression), enabling binary search within the block.
	DefaultRestartInterval = 16

	// DefaultBloomBitsPerKey is the default bloom filter bits per key.
	// 10 bits/key gives ~1% false positive rate.
	DefaultBloomBitsPerKey = 10

	// compressionNone indicates no compression.
	compressionNone byte = 0
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// BlockHandle locates a block within the SSTable file.
type BlockHandle struct {
	Offset uint64
	Length uint64
}

// encodedLen returns the number of bytes needed to encode this handle.
func (h BlockHandle) encodedLen() int {
	return varintLen(h.Offset) + varintLen(h.Length)
}

// encode appends the encoded handle to dst.
func (h BlockHandle) encode(dst []byte) int {
	n := binary.PutUvarint(dst, h.Offset)
	n += binary.PutUvarint(dst[n:], h.Length)
	return n
}

// decodeBlockHandle decodes a BlockHandle from src.
func decodeBlockHandle(src []byte) (BlockHandle, int) {
	offset, n1 := binary.Uvarint(src)
	length, n2 := binary.Uvarint(src[n1:])
	return BlockHandle{Offset: offset, Length: length}, n1 + n2
}

func varintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		n++
		x >>= 7
	}
	return n
}

// blockChecksum computes a CRC-32C checksum over the block data and type byte.
func blockChecksum(data []byte, blockType byte) uint32 {
	c := crc32.New(crcTable)
	c.Write(data)
	c.Write([]byte{blockType})
	return c.Sum32()
}

// TableMeta contains metadata about a written SSTable.
type TableMeta struct {
	// Size is the total file size in bytes.
	Size uint64
	// KeyCount is the number of key-value pairs.
	KeyCount uint64
	// SmallestKey is the smallest key in the table.
	SmallestKey []byte
	// LargestKey is the largest key in the table.
	LargestKey []byte
	// DataBlockCount is the number of data blocks.
	DataBlockCount int
}
