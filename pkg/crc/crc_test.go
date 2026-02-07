package crc

import (
	"hash/crc32"
	"testing"
)

var tab = crc32.MakeTable(crc32.Castagnoli)

func TestNew(t *testing.T) {
	h := New(0, tab)
	h.Write([]byte("hello"))
	crc1 := h.Sum32()

	if crc1 == 0 {
		t.Fatal("CRC should not be zero for non-empty input")
	}

	// Standard crc32 should match unseeded digest.
	want := crc32.Checksum([]byte("hello"), tab)
	if crc1 != want {
		t.Fatalf("CRC mismatch: got %08x, want %08x", crc1, want)
	}
}

func TestChaining(t *testing.T) {
	// Write "hello" then "world" through a single digest.
	h := New(0, tab)
	h.Write([]byte("hello"))
	h.Write([]byte("world"))
	combined := h.Sum32()

	// Seed a new digest with the CRC after "hello", then write "world".
	h1 := New(0, tab)
	h1.Write([]byte("hello"))
	mid := h1.Sum32()

	h2 := New(mid, tab)
	h2.Write([]byte("world"))
	chained := h2.Sum32()

	if combined != chained {
		t.Fatalf("Chained CRC mismatch: combined=%08x, chained=%08x", combined, chained)
	}
}

func TestReset(t *testing.T) {
	h := New(42, tab)
	h.Write([]byte("data"))
	h.Reset()

	// After reset, should behave like New(0, tab).
	h.Write([]byte("test"))
	got := h.Sum32()

	want := crc32.Checksum([]byte("test"), tab)
	if got != want {
		t.Fatalf("After Reset: got %08x, want %08x", got, want)
	}
}

func TestSize(t *testing.T) {
	h := New(0, tab)
	if h.Size() != Size {
		t.Fatalf("Size: got %d, want %d", h.Size(), Size)
	}
}

func TestSum(t *testing.T) {
	h := New(0, tab)
	h.Write([]byte("test"))
	sum := h.Sum(nil)
	if len(sum) != Size {
		t.Fatalf("Sum length: got %d, want %d", len(sum), Size)
	}
}
