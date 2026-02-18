package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// --- Block tests ---

func TestBlockWriter_RoundTrip(t *testing.T) {
	w := newBlockWriter(4) // restart every 4 keys

	entries := []struct{ key, val string }{
		{"apple", "1"},
		{"apricot", "2"},
		{"banana", "3"},
		{"blueberry", "4"},
		{"cherry", "5"},
		{"cranberry", "6"},
		{"date", "7"},
		{"elderberry", "8"},
	}

	for _, e := range entries {
		w.add([]byte(e.key), []byte(e.val))
	}
	data := w.finish()

	it := newBlockIterator(data)
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if i >= len(entries) {
			t.Fatal("too many entries")
		}
		if string(it.Key()) != entries[i].key {
			t.Errorf("entry %d: key = %q, want %q", i, it.Key(), entries[i].key)
		}
		if string(it.Value()) != entries[i].val {
			t.Errorf("entry %d: value = %q, want %q", i, it.Value(), entries[i].val)
		}
		i++
	}
	if i != len(entries) {
		t.Errorf("iterated %d entries, want %d", i, len(entries))
	}
}

func TestBlockWriter_SeekGE(t *testing.T) {
	w := newBlockWriter(4)
	keys := []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh"}
	for _, k := range keys {
		w.add([]byte(k), []byte(k))
	}
	data := w.finish()

	tests := []struct {
		seek string
		want string
	}{
		{"aa", "aa"},
		{"ab", "bb"},
		{"cc", "cc"},
		{"cd", "dd"},
		{"hh", "hh"},
		{"a", "aa"},
	}

	for _, tc := range tests {
		it := newBlockIterator(data)
		if !it.SeekGE([]byte(tc.seek)) {
			t.Errorf("SeekGE(%q): not found", tc.seek)
			continue
		}
		if string(it.Key()) != tc.want {
			t.Errorf("SeekGE(%q) = %q, want %q", tc.seek, it.Key(), tc.want)
		}
	}

	// Past end.
	it := newBlockIterator(data)
	if it.SeekGE([]byte("zz")) {
		t.Error("SeekGE(zz): should not be found")
	}
}

func TestBlockWriter_PrefixCompression(t *testing.T) {
	w := newBlockWriter(16)

	// Many keys with shared prefix — should be smaller than uncompressed.
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("prefix/long/path/key-%03d", i)
		w.add([]byte(k), []byte("v"))
	}
	data := w.finish()

	uncompressedSize := 0
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("prefix/long/path/key-%03d", i)
		uncompressedSize += len(k) + 1 // key + value "v"
	}

	// Block data should be significantly smaller due to prefix compression.
	if len(data) >= uncompressedSize {
		t.Errorf("prefix compression not working: block=%d, uncompressed=%d",
			len(data), uncompressedSize)
	}
}

// --- Bloom filter tests ---

func TestBloomFilter_RoundTrip(t *testing.T) {
	w := newBloomFilterWriter(10)
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%06d", i)
		w.addKey([]byte(keys[i]))
	}
	data := w.finish()

	f := decodeBloomFilter(data)
	if f == nil {
		t.Fatal("decodeBloomFilter returned nil")
	}

	// All inserted keys should be found (no false negatives).
	for _, k := range keys {
		if !f.mayContain([]byte(k)) {
			t.Errorf("false negative for %q", k)
		}
	}

	// Check false positive rate (should be ~1% with 10 bits/key).
	falsePositives := 0
	const nonExistentKeys = 10000
	for i := range nonExistentKeys {
		k := fmt.Sprintf("nonexistent-%06d", i)
		if f.mayContain([]byte(k)) {
			falsePositives++
		}
	}
	fpRate := float64(falsePositives) / nonExistentKeys
	if fpRate > 0.05 { // Allow up to 5% (generous threshold).
		t.Errorf("false positive rate too high: %.2f%%", fpRate*100)
	}
	t.Logf("false positive rate: %.2f%% (%d/%d)", fpRate*100, falsePositives, nonExistentKeys)
}

func TestBloomFilter_Empty(t *testing.T) {
	w := newBloomFilterWriter(10)
	data := w.finish()
	if data != nil {
		t.Error("empty filter should return nil")
	}

	f := decodeBloomFilter(nil)
	if f != nil {
		t.Error("decode nil should return nil")
	}
}

// --- Block cache tests ---

func TestBlockCache_Basic(t *testing.T) {
	c := NewBlockCache(1024)

	c.Set(1, 0, []byte("block0"))
	c.Set(1, 4096, []byte("block1"))

	if got := c.Get(1, 0); string(got) != "block0" {
		t.Errorf("Get(1,0) = %q, want block0", got)
	}
	if got := c.Get(1, 4096); string(got) != "block1" {
		t.Errorf("Get(1,4096) = %q, want block1", got)
	}
	if got := c.Get(2, 0); got != nil {
		t.Errorf("Get(2,0) = %q, want nil", got)
	}
}

func TestBlockCache_Eviction(t *testing.T) {
	c := NewBlockCache(100) // Small capacity.

	// Insert entries that exceed capacity.
	c.Set(1, 0, make([]byte, 50))
	c.Set(1, 1, make([]byte, 50))
	c.Set(1, 2, make([]byte, 50)) // Should evict oldest.

	// First entry should have been evicted.
	if got := c.Get(1, 0); got != nil {
		t.Error("entry 0 should have been evicted")
	}
	// Most recent should still be present.
	if got := c.Get(1, 2); got == nil {
		t.Error("entry 2 should still be cached")
	}
}

func TestBlockCache_LRUOrder(t *testing.T) {
	c := NewBlockCache(150)

	c.Set(1, 0, make([]byte, 50))
	c.Set(1, 1, make([]byte, 50))
	c.Set(1, 2, make([]byte, 50))

	// Access entry 0 to make it most recent.
	c.Get(1, 0)

	// Insert another entry — should evict entry 1 (least recently used).
	c.Set(1, 3, make([]byte, 50))

	if got := c.Get(1, 0); got == nil {
		t.Error("entry 0 should still be cached (recently accessed)")
	}
	if got := c.Get(1, 1); got != nil {
		t.Error("entry 1 should have been evicted (LRU)")
	}
}

func TestBlockCache_EvictFile(t *testing.T) {
	c := NewBlockCache(1024)

	c.Set(1, 0, []byte("a"))
	c.Set(1, 1, []byte("b"))
	c.Set(2, 0, []byte("c"))

	c.Evict(1)

	if got := c.Get(1, 0); got != nil {
		t.Error("file 1 entries should be evicted")
	}
	if got := c.Get(2, 0); got == nil {
		t.Error("file 2 entries should still be cached")
	}
}

// --- SSTable writer + reader round-trip tests ---

func TestSSTable_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sst")

	// Write.
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, DefaultWriterOptions())

	const n = 100
	for i := range n {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("val-%06d", i)
		if err := w.Add([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	meta, err := w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	f.Close()

	if meta.KeyCount != n {
		t.Errorf("KeyCount = %d, want %d", meta.KeyCount, n)
	}
	if string(meta.SmallestKey) != "key-000000" {
		t.Errorf("SmallestKey = %q", meta.SmallestKey)
	}
	if string(meta.LargestKey) != "key-000099" {
		t.Errorf("LargestKey = %q", meta.LargestKey)
	}

	// Read.
	cache := NewBlockCache(1 << 20) // 1MB cache
	reader, rf, err := OpenReader(path, ReaderOptions{Cache: cache, FileID: 1})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer rf.Close()

	// Point lookups.
	for i := range n {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("val-%06d", i)
		got, err := reader.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if string(got) != v {
			t.Errorf("Get(%q) = %q, want %q", k, got, v)
		}
	}

	// Missing key (bloom filter should reject quickly).
	_, err = reader.Get([]byte("missing"))
	if err != ErrNotFound {
		t.Errorf("Get(missing): %v, want ErrNotFound", err)
	}
}

func TestSSTable_Iterator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, DefaultWriterOptions())

	const n = 50
	for i := range n {
		k := fmt.Sprintf("key-%06d", i)
		w.Add([]byte(k), []byte(k))
	}
	w.Close()
	f.Close()

	reader, rf, err := OpenReader(path, ReaderOptions{FileID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	// Full scan.
	it := reader.NewIterator()
	count := 0
	var prev []byte
	for it.First(); it.Valid(); it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("out of order: %q >= %q", prev, it.Key())
		}
		prev = append(prev[:0], it.Key()...)
		count++
	}
	if count != n {
		t.Errorf("iterated %d, want %d", count, n)
	}

	// SeekGE.
	it = reader.NewIterator()
	if !it.SeekGE([]byte("key-000025")) {
		t.Fatal("SeekGE(key-000025): not found")
	}
	if string(it.Key()) != "key-000025" {
		t.Errorf("SeekGE key = %q, want key-000025", it.Key())
	}

	// Count remaining.
	remaining := 0
	for it.Valid() {
		remaining++
		it.Next()
	}
	if remaining != 25 { // keys 25-49
		t.Errorf("remaining from 25: %d, want 25", remaining)
	}
}

func TestSSTable_LargeTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.sst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, DefaultWriterOptions())

	const n = 10000
	for i := range n {
		k := fmt.Sprintf("key-%08d", i)
		v := fmt.Sprintf("value-%08d-padding-to-make-this-longer", i)
		w.Add([]byte(k), []byte(v))
	}
	meta, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	t.Logf("table: %d bytes, %d data blocks, %d keys", meta.Size, meta.DataBlockCount, meta.KeyCount)

	cache := NewBlockCache(1 << 20)
	reader, rf, err := OpenReader(path, ReaderOptions{Cache: cache, FileID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	// Spot check some keys.
	for _, i := range []int{0, 1, 999, 5000, 9999} {
		k := fmt.Sprintf("key-%08d", i)
		v := fmt.Sprintf("value-%08d-padding-to-make-this-longer", i)
		got, err := reader.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if string(got) != v {
			t.Errorf("Get(%q) = %q, want %q", k, got, v)
		}
	}

	// Full iteration.
	it := reader.NewIterator()
	count := 0
	for it.First(); it.Valid(); it.Next() {
		count++
	}
	if count != n {
		t.Errorf("full scan: %d keys, want %d", count, n)
	}
}

func TestSSTable_EmptyTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.sst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, DefaultWriterOptions())
	meta, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	if meta.KeyCount != 0 {
		t.Errorf("KeyCount = %d, want 0", meta.KeyCount)
	}

	reader, rf, err := OpenReader(path, ReaderOptions{FileID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	_, err = reader.Get([]byte("anything"))
	if err != ErrNotFound {
		t.Errorf("Get on empty: %v, want ErrNotFound", err)
	}
}

func TestSSTable_BloomRejectsNonExistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bloom.sst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, DefaultWriterOptions())

	// Only write keys with "existing-" prefix.
	for i := range 1000 {
		k := fmt.Sprintf("existing-%06d", i)
		w.Add([]byte(k), []byte("v"))
	}
	w.Close()
	f.Close()

	reader, rf, err := OpenReader(path, ReaderOptions{FileID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	// Test that non-existent keys are efficiently rejected.
	rejected := 0
	for i := range 1000 {
		k := fmt.Sprintf("nonexistent-%06d", i)
		_, err := reader.Get([]byte(k))
		if err == ErrNotFound {
			rejected++
		}
	}
	if rejected != 1000 {
		t.Errorf("expected all 1000 non-existent keys to return ErrNotFound, got %d", rejected)
	}
}

func TestSSTable_OutOfOrderKeys(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, DefaultWriterOptions())

	w.Add([]byte("b"), []byte("1"))
	err := w.Add([]byte("a"), []byte("2"))
	if err == nil {
		t.Error("expected error for out-of-order keys")
	}
}

// --- Benchmarks ---

func BenchmarkSSTable_Get(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.sst")

	f, _ := os.Create(path)
	w := NewWriter(f, DefaultWriterOptions())
	const n = 100000
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		w.Add(keys[i], keys[i])
	}
	w.Close()
	f.Close()

	cache := NewBlockCache(64 << 20) // 64MB
	reader, rf, _ := OpenReader(path, ReaderOptions{Cache: cache, FileID: 1})
	defer rf.Close()

	b.ResetTimer()
	for i := range b.N {
		reader.Get(keys[i%n])
	}
}
