package sstable

import (
	"encoding/binary"
	"math"
)

const (
	cacheLineSize = 64    // bytes
	cacheLineBits = 512   // 64 * 8
)

// bloomFilterWriter accumulates key hashes and builds a partitioned bloom filter.
// The filter is organized into cache-line-sized (64-byte) blocks for CPU efficiency.
//
// Filter format:
//
//	[filter data: nLines * 64 bytes]
//	[num_probes: 1 byte]
//	[nLines: 4 bytes, little-endian]
//
// Pebble reference: internal/bloom/bloom.go
type bloomFilterWriter struct {
	hashes     []uint32
	bitsPerKey int
}

func newBloomFilterWriter(bitsPerKey int) *bloomFilterWriter {
	return &bloomFilterWriter{bitsPerKey: bitsPerKey}
}

// addKey hashes a key and stores the hash for later filter construction.
func (w *bloomFilterWriter) addKey(key []byte) {
	w.hashes = append(w.hashes, bloomHash(key))
}

// finish builds the bloom filter and returns the encoded filter block.
func (w *bloomFilterWriter) finish() []byte {
	if len(w.hashes) == 0 {
		return nil
	}

	numProbes := calcNumProbes(w.bitsPerKey)
	nLines := calcNLines(len(w.hashes), w.bitsPerKey)

	// Allocate filter data + 1-byte numProbes + 4-byte nLines.
	filterSize := nLines * cacheLineSize
	buf := make([]byte, filterSize+5)

	for _, h := range w.hashes {
		delta := (h >> 17) | (h << 15)
		lineIdx := h % uint32(nLines)
		lineOff := int(lineIdx) * cacheLineSize
		for j := 0; j < numProbes; j++ {
			byteIdx := (h / 8) % cacheLineSize
			bitIdx := h % 8
			buf[lineOff+int(byteIdx)] |= 1 << bitIdx
			h += delta
		}
	}

	buf[filterSize] = byte(numProbes)
	binary.LittleEndian.PutUint32(buf[filterSize+1:], uint32(nLines))

	return buf
}

// reset clears accumulated hashes for the next block.
func (w *bloomFilterWriter) reset() {
	w.hashes = w.hashes[:0]
}

// bloomFilter probes an encoded bloom filter for a key.
type bloomFilter struct {
	data      []byte
	numProbes int
	nLines    int
}

// decodeBloomFilter parses an encoded bloom filter block.
func decodeBloomFilter(data []byte) *bloomFilter {
	if len(data) < 5 {
		return nil
	}
	numProbes := int(data[len(data)-5])
	nLines := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	filterData := data[:len(data)-5]

	if len(filterData) != nLines*cacheLineSize {
		return nil
	}

	return &bloomFilter{
		data:      filterData,
		numProbes: numProbes,
		nLines:    nLines,
	}
}

// mayContain returns true if the key may be in the filter (probabilistic).
// Returns false only if the key is definitely NOT in the set.
func (f *bloomFilter) mayContain(key []byte) bool {
	if f == nil || f.nLines == 0 {
		return true // No filter — assume present.
	}

	h := bloomHash(key)
	delta := (h >> 17) | (h << 15)
	lineIdx := h % uint32(f.nLines)
	lineOff := int(lineIdx) * cacheLineSize

	for j := 0; j < f.numProbes; j++ {
		byteIdx := (h / 8) % cacheLineSize
		bitIdx := h % 8
		if f.data[lineOff+int(byteIdx)]&(1<<bitIdx) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// bloomHash computes a Murmur-like hash for bloom filter keys.
func bloomHash(key []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(uint64(len(key))*m)

	i := 0
	for i+4 <= len(key) {
		h += binary.LittleEndian.Uint32(key[i:])
		h *= m
		h ^= h >> 16
		i += 4
	}

	switch len(key) - i {
	case 3:
		h += uint32(int8(key[i+2])) << 16
		fallthrough
	case 2:
		h += uint32(int8(key[i+1])) << 8
		fallthrough
	case 1:
		h += uint32(int8(key[i]))
		h *= m
		h ^= h >> 24
	}

	return h
}

func calcNumProbes(bitsPerKey int) int {
	// Optimal probes by bits-per-key (from Pebble simulation data).
	n := int(float64(bitsPerKey) * math.Ln2)
	if n < 1 {
		n = 1
	}
	if n > 30 {
		n = 30
	}
	return n
}

func calcNLines(numKeys, bitsPerKey int) int {
	nBits := numKeys * bitsPerKey
	nLines := (nBits + cacheLineBits - 1) / cacheLineBits
	if nLines < 1 {
		nLines = 1
	}
	// Make nLines odd for better distribution.
	if nLines%2 == 0 {
		nLines++
	}
	return nLines
}
