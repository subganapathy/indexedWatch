package lsm

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/subganapathy/indexedwatch/pkg/lsm/sstable"
)

func testOptions() Options {
	return Options{
		MemtableSize:   4 * 1024, // 4 KB — small for testing rotation
		BlockCacheSize: 64 * 1024,
		TableOptions:   sstable.DefaultWriterOptions(),
	}
}

func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db, dir
}

func TestOpenClose(t *testing.T) {
	db, _ := openTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Double close returns ErrClosed.
	if err := db.Close(); err != ErrClosed {
		t.Fatalf("double Close: got %v, want ErrClosed", err)
	}
}

func TestSetGet(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	if err := db.Set([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("Get: got %q, want %q", val, "world")
	}
}

func TestGetNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	_, err := db.Get([]byte("missing"))
	if err != ErrNotFound {
		t.Fatalf("Get missing: got %v, want ErrNotFound", err)
	}
}

func TestDelete(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Set([]byte("key"), []byte("val"))

	val, err := db.Get([]byte("key"))
	if err != nil || string(val) != "val" {
		t.Fatalf("Get before delete: %v %q", err, val)
	}

	db.Delete([]byte("key"))

	_, err = db.Get([]byte("key"))
	if err != ErrNotFound {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestDeleteShadowsOlderVersions(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	// Write key, then delete it, then check — the delete should
	// shadow the older set even if both are in the same memtable.
	db.Set([]byte("k"), []byte("v1"))
	db.Delete([]byte("k"))

	_, err := db.Get([]byte("k"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	// Re-set the key.
	db.Set([]byte("k"), []byte("v2"))
	val, err := db.Get([]byte("k"))
	if err != nil || string(val) != "v2" {
		t.Fatalf("Get after re-set: %v %q", err, val)
	}
}

func TestOverwrite(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Set([]byte("key"), []byte("v1"))
	db.Set([]byte("key"), []byte("v2"))
	db.Set([]byte("key"), []byte("v3"))

	val, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "v3" {
		t.Fatalf("Get: got %q, want %q", val, "v3")
	}
}

func TestManyKeys(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	n := 200
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		if err := db.Set([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		expected := fmt.Sprintf("val-%04d", i)
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Get %s: got %q, want %q", key, val, expected)
		}
	}
}

func TestMemtableRotationAndFlush(t *testing.T) {
	db, dir := openTestDB(t)
	defer db.Close()

	// Write enough data to trigger memtable rotation + flush.
	// With 4 KB memtable, a few hundred entries should trigger rotation.
	n := 500
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("value-%06d-padding-to-increase-size", i)
		if err := db.Set([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	// Wait for flushes to complete by writing one more entry and reading it.
	db.Set([]byte("sentinel"), []byte("done"))

	// Verify all keys are readable.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		expected := fmt.Sprintf("value-%06d-padding-to-increase-size", i)
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Get %s: got %q, want %q", key, val, expected)
		}
	}

	// Verify SSTable files were created.
	files, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(files) == 0 {
		t.Fatal("expected SSTable files after flush, got none")
	}
}

func TestRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()

	// Write data.
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	n := 300
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		db.Set([]byte(key), []byte(val))
	}

	// Wait for flushes.
	db.Set([]byte("final"), []byte("entry"))

	db.Close()

	// Reopen and verify data persisted in SSTables.
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Keys that were flushed to SSTables should be readable.
	// (Keys still in the memtable at close time are lost without WAL —
	// the LSM layer relies on the WAL layer above for that.)
	found := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		expected := fmt.Sprintf("val-%04d", i)
		val, err := db2.Get([]byte(key))
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			t.Fatalf("Get %s after recovery: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Get %s after recovery: got %q, want %q", key, val, expected)
		}
		found++
	}

	if found == 0 {
		t.Fatal("no keys survived recovery — flush may not have completed")
	}
	t.Logf("recovered %d/%d keys (rest were in unflushed memtable)", found, n)
}

func TestIterator(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		db.Set([]byte(k), []byte("v-"+k))
	}

	it := db.NewIterator()
	defer it.Close()

	var got []string
	for it.First(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
		expected := "v-" + string(it.Key())
		if string(it.Value()) != expected {
			t.Fatalf("iterator value for %s: got %q, want %q", it.Key(), it.Value(), expected)
		}
	}

	if len(got) != len(keys) {
		t.Fatalf("iterator count: got %d, want %d", len(got), len(keys))
	}
	for i, k := range got {
		if k != keys[i] {
			t.Fatalf("iterator key %d: got %q, want %q", i, k, keys[i])
		}
	}
}

func TestIteratorSeekGE(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%02d", i)
		db.Set([]byte(key), []byte(fmt.Sprintf("val-%02d", i)))
	}

	it := db.NewIterator()
	defer it.Close()

	// Seek to key-05.
	if !it.SeekGE([]byte("key-05")) {
		t.Fatal("SeekGE(key-05) returned false")
	}
	if string(it.Key()) != "key-05" {
		t.Fatalf("SeekGE: got %q, want %q", it.Key(), "key-05")
	}

	// Seek to a key between existing keys.
	if !it.SeekGE([]byte("key-03x")) {
		t.Fatal("SeekGE(key-03x) returned false")
	}
	if string(it.Key()) != "key-04" {
		t.Fatalf("SeekGE(key-03x): got %q, want %q", it.Key(), "key-04")
	}

	// Seek past all keys.
	if it.SeekGE([]byte("zzz")) {
		t.Fatal("SeekGE(zzz) should return false")
	}
}

func TestIteratorSkipsTombstones(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Set([]byte("a"), []byte("va"))
	db.Set([]byte("b"), []byte("vb"))
	db.Set([]byte("c"), []byte("vc"))
	db.Delete([]byte("b"))

	it := db.NewIterator()
	defer it.Close()

	var got []string
	for it.First(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}

	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("iterator after delete: got %v, want [a c]", got)
	}
}

func TestIteratorMVCC(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	// Write multiple versions — iterator should show only the latest.
	db.Set([]byte("key"), []byte("v1"))
	db.Set([]byte("key"), []byte("v2"))
	db.Set([]byte("key"), []byte("v3"))

	it := db.NewIterator()
	defer it.Close()

	if !it.First() {
		t.Fatal("iterator: First returned false")
	}
	if string(it.Key()) != "key" {
		t.Fatalf("iterator key: got %q, want %q", it.Key(), "key")
	}
	if string(it.Value()) != "v3" {
		t.Fatalf("iterator value: got %q, want %q (should be newest)", it.Value(), "v3")
	}
	if it.Next() {
		t.Fatalf("iterator: unexpected second entry: %q=%q", it.Key(), it.Value())
	}
}

func TestSnapshot(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Set([]byte("key"), []byte("v1"))

	snap := db.NewSnapshot()
	defer snap.Close()

	// Write new version after snapshot.
	db.Set([]byte("key"), []byte("v2"))

	// Snapshot should still see v1.
	val, err := snap.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Snapshot.Get: %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("Snapshot.Get: got %q, want %q", val, "v1")
	}

	// Current DB should see v2.
	val, err = db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("DB.Get: %v", err)
	}
	if string(val) != "v2" {
		t.Fatalf("DB.Get: got %q, want %q", val, "v2")
	}
}

func TestSnapshotDelete(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Set([]byte("key"), []byte("val"))

	snap := db.NewSnapshot()
	defer snap.Close()

	db.Delete([]byte("key"))

	// Snapshot should still see the key.
	val, err := snap.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Snapshot.Get after delete: %v", err)
	}
	if string(val) != "val" {
		t.Fatalf("Snapshot.Get: got %q, want %q", val, "val")
	}

	// Current DB should not see it.
	_, err = db.Get([]byte("key"))
	if err != ErrNotFound {
		t.Fatalf("DB.Get after delete: expected ErrNotFound, got %v", err)
	}
}

func TestLastAppliedSeqNum(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	before := db.LastAppliedSeqNum()
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	after := db.LastAppliedSeqNum()

	if after != before+2 {
		t.Fatalf("seqNum: got %d, want %d", after, before+2)
	}
}

func TestConcurrentReadsWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		MemtableSize:   64 * 1024, // 64 KB — larger for concurrent test stability
		BlockCacheSize: 64 * 1024,
		TableOptions:   sstable.DefaultWriterOptions(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	n := 100

	// Writers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("w%d-key-%04d", id, i)
				val := fmt.Sprintf("w%d-val-%04d", id, i)
				if err := db.Set([]byte(key), []byte(val)); err != nil {
					t.Errorf("Set %s: %v", key, err)
				}
			}
		}(w)
	}

	// Readers (reading keys that may or may not exist yet).
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("w%d-key-%04d", id, i)
				db.Get([]byte(key))
			}
		}(r)
	}

	wg.Wait()

	// Verify all written keys are readable.
	for w := 0; w < 4; w++ {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("w%d-key-%04d", w, i)
			expected := fmt.Sprintf("w%d-val-%04d", w, i)
			val, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("Get %s: %v", key, err)
			}
			if string(val) != expected {
				t.Fatalf("Get %s: got %q, want %q", key, val, expected)
			}
		}
	}
}

func TestClosedDBOperations(t *testing.T) {
	db, _ := openTestDB(t)
	db.Close()

	if _, err := db.Get([]byte("key")); err != ErrClosed {
		t.Fatalf("Get on closed DB: got %v, want ErrClosed", err)
	}
	if err := db.Set([]byte("key"), []byte("val")); err != ErrClosed {
		t.Fatalf("Set on closed DB: got %v, want ErrClosed", err)
	}
	if err := db.Delete([]byte("key")); err != ErrClosed {
		t.Fatalf("Delete on closed DB: got %v, want ErrClosed", err)
	}
}

func TestEmptyIterator(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	it := db.NewIterator()
	defer it.Close()

	if it.First() {
		t.Fatal("First on empty DB should return false")
	}
	if it.Valid() {
		t.Fatal("Valid on empty DB should return false")
	}
}

func TestManifestPersistence(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()

	// First run: write data that will be flushed.
	db, _ := Open(dir, opts)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("persist-%06d", i)
		val := fmt.Sprintf("value-%06d-padding-to-trigger-flush-rotate", i)
		db.Set([]byte(key), []byte(val))
	}
	db.Close()

	// Verify MANIFEST exists.
	manifestPath := filepath.Join(dir, "MANIFEST")
	info, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("MANIFEST stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("MANIFEST is empty")
	}
}

func TestLargeValues(t *testing.T) {
	dir := t.TempDir()
	// Need a larger memtable to hold the big value + node overhead.
	db, err := Open(dir, Options{
		MemtableSize:   64 * 1024, // 64 KB
		BlockCacheSize: 64 * 1024,
		TableOptions:   sstable.DefaultWriterOptions(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write a value larger than a block.
	bigVal := bytes.Repeat([]byte("x"), 8192)
	if err := db.Set([]byte("big"), bigVal); err != nil {
		t.Fatalf("Set big: %v", err)
	}

	val, err := db.Get([]byte("big"))
	if err != nil {
		t.Fatalf("Get big: %v", err)
	}
	if !bytes.Equal(val, bigVal) {
		t.Fatalf("Get big: length %d, want %d", len(val), len(bigVal))
	}
}

func TestSequentialDeleteInsert(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	// Cycle through delete and re-insert.
	for cycle := 0; cycle < 5; cycle++ {
		db.Set([]byte("cycling"), []byte(fmt.Sprintf("cycle-%d", cycle)))
		val, err := db.Get([]byte("cycling"))
		if err != nil {
			t.Fatalf("cycle %d Get: %v", cycle, err)
		}
		if string(val) != fmt.Sprintf("cycle-%d", cycle) {
			t.Fatalf("cycle %d: got %q", cycle, val)
		}
		db.Delete([]byte("cycling"))
		_, err = db.Get([]byte("cycling"))
		if err != ErrNotFound {
			t.Fatalf("cycle %d after delete: got %v", cycle, err)
		}
	}
}

func TestPrefixScan(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	// Write keys with different prefixes.
	prefixes := []string{"user/", "order/", "product/"}
	for _, p := range prefixes {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("%s%04d", p, i)
			db.Set([]byte(key), []byte("val"))
		}
	}

	// Scan for "order/" prefix.
	it := db.NewIterator()
	defer it.Close()

	var orderKeys []string
	for it.SeekGE([]byte("order/")); it.Valid(); it.Next() {
		k := string(it.Key())
		if k >= "order0" { // past the prefix
			break
		}
		orderKeys = append(orderKeys, k)
	}

	if len(orderKeys) != 5 {
		t.Fatalf("prefix scan: got %d keys, want 5: %v", len(orderKeys), orderKeys)
	}
}

func TestRandomOperations(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	rng := rand.New(rand.NewSource(42))
	reference := make(map[string]string) // ground truth

	n := 500
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("rnd-%04d", rng.Intn(100))
		if rng.Float64() < 0.2 {
			// Delete.
			db.Delete([]byte(key))
			delete(reference, key)
		} else {
			// Set.
			val := fmt.Sprintf("val-%d", i)
			db.Set([]byte(key), []byte(val))
			reference[key] = val
		}
	}

	// Verify all entries match reference.
	for key, expected := range reference {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Get %s: got %q, want %q", key, val, expected)
		}
	}

	// Verify deleted keys are gone.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("rnd-%04d", i)
		if _, ok := reference[key]; ok {
			continue
		}
		_, err := db.Get([]byte(key))
		if err != ErrNotFound {
			t.Fatalf("Get deleted %s: got %v, want ErrNotFound", key, err)
		}
	}
}

func BenchmarkSet(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, Options{
		MemtableSize:   32 * 1024 * 1024,
		BlockCacheSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-%08d", i)
		db.Set([]byte(key), val)
	}
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, Options{
		MemtableSize:   32 * 1024 * 1024,
		BlockCacheSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepopulate.
	n := 10000
	val := bytes.Repeat([]byte("v"), 100)
	for i := 0; i < n; i++ {
		db.Set([]byte(fmt.Sprintf("bench-%08d", i)), val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-%08d", i%n)
		db.Get([]byte(key))
	}
}
