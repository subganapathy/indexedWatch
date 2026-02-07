// Package crc provides a seedable CRC-32C (Castagnoli) digest.
//
// The key feature is New(prev, tab) which seeds the digest with a previous
// CRC value, enabling chaining across WAL records and segment files.
// CRC-32C is chosen for better error detection than IEEE CRC-32 and
// hardware acceleration via SSE4.2/ARM CRC on modern CPUs.
package crc

import (
	"hash"
	"hash/crc32"
)

// Size is the size of a CRC-32 checksum in bytes.
const Size = 4

type digest struct {
	crc uint32
	tab *crc32.Table
}

// New creates a new hash.Hash32 computing the CRC-32 checksum using the
// polynomial represented by the Table, seeded with prev. This enables
// chaining: the CRC of record N includes all prior records' data.
func New(prev uint32, tab *crc32.Table) hash.Hash32 { return &digest{prev, tab} }

func (d *digest) Size() int      { return Size }
func (d *digest) BlockSize() int  { return 1 }
func (d *digest) Reset()          { d.crc = 0 }

func (d *digest) Write(p []byte) (n int, err error) {
	d.crc = crc32.Update(d.crc, d.tab, p)
	return len(p), nil
}

func (d *digest) Sum32() uint32 { return d.crc }

func (d *digest) Sum(in []byte) []byte {
	s := d.Sum32()
	return append(in, byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}
