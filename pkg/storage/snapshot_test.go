package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/subganapathy/indexedwatch/pkg/lsm"
)

func TestTakeSnapshot(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	// Write some data.
	for i := 0; i < 20; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf(`{"id":"%s","name":"val-%d"}`, pk, i)
		if _, err := ts.Create([]byte(pk), []byte(data)); err != nil {
			t.Fatalf("create %s: %v", pk, err)
		}
	}

	// Delete a few.
	r5, _ := ts.Get([]byte("key-0005"))
	ts.Delete([]byte("key-0005"), r5.UpdateSeqNum)
	r10, _ := ts.Get([]byte("key-0010"))
	ts.Delete([]byte("key-0010"), r10.UpdateSeqNum)

	// Take snapshot.
	meta, err := ts.TakeSnapshot()
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	if meta.SeqNum != ts.LastSeqNum() {
		t.Fatalf("snapshot seqNum: expected %d, got %d", ts.LastSeqNum(), meta.SeqNum)
	}

	// Verify snapshot file exists.
	if _, err := os.Stat(meta.Path); err != nil {
		t.Fatalf("snapshot file not found: %v", err)
	}

	// Verify snapshot.meta exists.
	storedMeta := readSnapshotMeta(ts.dir)
	if storedMeta == nil {
		t.Fatal("snapshot.meta not found")
	}
	if storedMeta.SeqNum != meta.SeqNum {
		t.Fatalf("meta seqNum: expected %d, got %d", meta.SeqNum, storedMeta.SeqNum)
	}
}

func TestRecoveryFromSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "types", "events")
	reg := setupRegistry(t)
	opts := testTypeStoreOpts()

	// Phase 1: create records and take a snapshot.
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 10; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf(`{"id":"%s","name":"val-%d"}`, pk, i)
		if _, err := ts.Create([]byte(pk), []byte(data)); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	_, err = ts.TakeSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Write more data after snapshot.
	for i := 10; i < 15; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf(`{"id":"%s","name":"val-%d"}`, pk, i)
		if _, err := ts.Create([]byte(pk), []byte(data)); err != nil {
			t.Fatalf("create post-snapshot: %v", err)
		}
	}

	lastSeq := ts.LastSeqNum()
	ts.Close()

	// Phase 2: reopen — should recover from snapshot + WAL replay.
	ts2, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	// Verify seqNum restored.
	if ts2.LastSeqNum() != lastSeq {
		t.Fatalf("seqNum: expected %d, got %d", lastSeq, ts2.LastSeqNum())
	}

	// Verify all 15 records exist (10 from snapshot + 5 from WAL replay).
	for i := 0; i < 15; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		record, err := ts2.Get([]byte(pk))
		if err != nil {
			t.Fatalf("get %s: %v", pk, err)
		}
		expected := fmt.Sprintf(`{"id":"%s","name":"val-%d"}`, pk, i)
		if string(record.Data) != expected {
			t.Fatalf("data mismatch for %s: %q", pk, record.Data)
		}
	}
}

func TestSnapshotExcludesDeleted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "types", "events")
	reg := setupRegistry(t)
	opts := testTypeStoreOpts()

	// Phase 1: create, delete some, snapshot.
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	seq1, _ := ts.Create([]byte("key1"), []byte(`{"id":"key1","name":"a"}`))
	ts.Create([]byte("key2"), []byte(`{"id":"key2","name":"b"}`))
	ts.Delete([]byte("key1"), seq1) // delete key1

	ts.TakeSnapshot()
	ts.Close()

	// Phase 2: reopen — key1 should still be not found.
	ts2, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	if _, err := ts2.Get([]byte("key1")); err != ErrNotFound {
		t.Fatalf("key1 should be not found, got: %v", err)
	}

	r2, err := ts2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("get key2: %v", err)
	}
	if string(r2.Data) != `{"id":"key2","name":"b"}` {
		t.Fatalf("key2 data: %q", r2.Data)
	}
}

func TestSnapshotFileFormat(t *testing.T) {
	// Test snapshot write + load round-trip at the low level.
	lsmDir := filepath.Join(t.TempDir(), "lsm")
	snapDir := t.TempDir()

	// Create LSM with some data.
	db, err := lsm.Open(lsmDir, lsm.Options{
		MemtableSize:   64 * 1024,
		BlockCacheSize: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("open lsm: %v", err)
	}

	records := map[string]*StoredRecord{
		"pk-1": {Data: []byte("data1"), CreateSeqNum: 1, UpdateSeqNum: 1},
		"pk-2": {Data: []byte("data2"), CreateSeqNum: 2, UpdateSeqNum: 3},
		"pk-3": {IsDeleted: true, CreateSeqNum: 4, UpdateSeqNum: 5},
	}

	for pk, rec := range records {
		if err := db.Set([]byte(pk), rec.Marshal()); err != nil {
			t.Fatalf("set %s: %v", pk, err)
		}
	}

	// Take snapshot.
	it := db.NewIterator()
	path, err := takeSnapshot(it, snapDir, 42)
	it.Close()
	if err != nil {
		t.Fatalf("takeSnapshot: %v", err)
	}
	db.Close()

	// Load snapshot into fresh LSM.
	lsmDir2 := filepath.Join(t.TempDir(), "lsm2")
	db2, err := lsm.Open(lsmDir2, lsm.Options{
		MemtableSize:   64 * 1024,
		BlockCacheSize: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("open lsm2: %v", err)
	}
	defer db2.Close()

	seqNum, err := loadSnapshot(path, db2)
	if err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}
	if seqNum != 42 {
		t.Fatalf("seqNum: expected 42, got %d", seqNum)
	}

	// Verify pk-1 and pk-2 exist (pk-3 was deleted, excluded from snapshot).
	val, err := db2.Get([]byte("pk-1"))
	if err != nil {
		t.Fatalf("get pk-1: %v", err)
	}
	r, _ := UnmarshalStoredRecord(val)
	if string(r.Data) != "data1" {
		t.Fatalf("pk-1 data: %q", r.Data)
	}

	val, err = db2.Get([]byte("pk-2"))
	if err != nil {
		t.Fatalf("get pk-2: %v", err)
	}
	r, _ = UnmarshalStoredRecord(val)
	if string(r.Data) != "data2" || r.CreateSeqNum != 2 || r.UpdateSeqNum != 3 {
		t.Fatalf("pk-2: %+v", r)
	}

	// pk-3 should not exist (was deleted, excluded from snapshot).
	if _, err := db2.Get([]byte("pk-3")); err != lsm.ErrNotFound {
		t.Fatalf("pk-3 should not exist: %v", err)
	}
}

func TestMultipleSnapshots(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "types", "events")
	reg := setupRegistry(t)
	opts := testTypeStoreOpts()

	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ts.Close()

	// Write + snapshot multiple times.
	for round := 0; round < 3; round++ {
		for i := 0; i < 5; i++ {
			pk := fmt.Sprintf("r%d-key-%d", round, i)
			data := fmt.Sprintf(`{"id":"%s","name":"v%d"}`, pk, round)
			ts.Create([]byte(pk), []byte(data))
		}
		meta, err := ts.TakeSnapshot()
		if err != nil {
			t.Fatalf("snapshot round %d: %v", round, err)
		}

		// Only the latest snapshot file should exist.
		snapDir := filepath.Join(dir, snapshotDir)
		entries, _ := os.ReadDir(snapDir)
		if len(entries) != 1 {
			t.Fatalf("round %d: expected 1 snapshot file, got %d", round, len(entries))
		}
		if entries[0].Name() != filepath.Base(meta.Path) {
			t.Fatalf("round %d: wrong snapshot file", round)
		}
	}

	// Verify all 15 records exist.
	allEntries, err := ts.List(nil, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(allEntries) != 15 {
		t.Fatalf("expected 15 entries, got %d", len(allEntries))
	}
}

func TestSnapshotCorruptedFallsBack(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "types", "events")
	reg := setupRegistry(t)
	opts := testTypeStoreOpts()

	// Phase 1: create data and snapshot.
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ts.Create([]byte("key1"), []byte(`{"id":"key1","name":"a"}`))
	ts.Create([]byte("key2"), []byte(`{"id":"key2","name":"b"}`))

	meta, err := ts.TakeSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	ts.Close()

	// Corrupt the snapshot file.
	if err := os.WriteFile(meta.Path, []byte("corrupted"), 0644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	// Phase 2: reopen — should fall back to full WAL replay.
	ts2, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	// Data should still be recoverable from WAL.
	r1, err := ts2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get key1: %v", err)
	}
	if string(r1.Data) != `{"id":"key1","name":"a"}` {
		t.Fatalf("key1 data: %q", r1.Data)
	}
}
