package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/subganapathy/indexedwatch/pkg/lsm"
	"github.com/subganapathy/indexedwatch/pkg/schema"
	"github.com/subganapathy/indexedwatch/pkg/wal"
)

// testSchema creates a minimal schema for testing.
func testSchema() *schema.Schema {
	return &schema.Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "id",
		Fields: map[string]schema.Field{
			"id":   {Type: 1, Required: true}, // FIELD_TYPE_STRING
			"name": {Type: 1},
		},
	}
}

// setupRegistry creates a schema registry with a registered and current schema.
func setupRegistry(t *testing.T) *schema.Registry {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "schema")
	reg, err := schema.Open(dir)
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	s := testSchema()
	if err := reg.Register(s); err != nil {
		t.Fatalf("register schema: %v", err)
	}
	if err := reg.SetCurrent(s.Type, s.Version); err != nil {
		t.Fatalf("set current: %v", err)
	}
	t.Cleanup(func() { reg.Close() })
	return reg
}

// testTypeStoreOpts returns small options suitable for testing.
func testTypeStoreOpts() TypeStoreOptions {
	return TypeStoreOptions{
		LSMOptions: lsm.Options{
			MemtableSize:   64 * 1024, // 64KB for fast rotation
			BlockCacheSize: 64 * 1024,
		},
		WALOptions: wal.DefaultOptions(),
	}
}

// openTestTypeStore opens a TypeStore in a temp directory.
func openTestTypeStore(t *testing.T, reg *schema.Registry) *TypeStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "types", "events")
	ts, err := OpenTypeStore(dir, "events", reg, testTypeStoreOpts())
	if err != nil {
		t.Fatalf("open type store: %v", err)
	}
	t.Cleanup(func() { ts.Close() })
	return ts
}

func TestCreateAndGet(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	seqNum, err := ts.Create([]byte("key1"), []byte(`{"id":"key1","name":"alice"}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if seqNum != 1 {
		t.Fatalf("expected seqNum=1, got %d", seqNum)
	}

	record, err := ts.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(record.Data) != `{"id":"key1","name":"alice"}` {
		t.Fatalf("unexpected data: %s", record.Data)
	}
	if record.SchemaVersion != "v1" {
		t.Fatalf("unexpected schema version: %s", record.SchemaVersion)
	}
	if record.CreateSeqNum != 1 || record.UpdateSeqNum != 1 {
		t.Fatalf("unexpected seqNums: create=%d update=%d", record.CreateSeqNum, record.UpdateSeqNum)
	}
	if record.IsDeleted {
		t.Fatal("record should not be deleted")
	}
}

func TestGetNotFound(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	_, err := ts.Get([]byte("nonexistent"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestCreateDuplicate(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	if _, err := ts.Create([]byte("key1"), []byte("data1")); err != nil {
		t.Fatalf("first create: %v", err)
	}

	_, err := ts.Create([]byte("key1"), []byte("data2"))
	if err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got: %v", err)
	}
}

func TestCreateAfterDelete(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	seqNum, err := ts.Create([]byte("key1"), []byte("data1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := ts.Delete([]byte("key1"), seqNum); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Create after delete should succeed (re-create).
	seqNum2, err := ts.Create([]byte("key1"), []byte("data2"))
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	if seqNum2 != 3 {
		t.Fatalf("expected seqNum=3, got %d", seqNum2)
	}

	record, err := ts.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(record.Data) != "data2" {
		t.Fatalf("unexpected data: %s", record.Data)
	}
}

func TestUpdate(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	createSeq, err := ts.Create([]byte("key1"), []byte("original"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	updateSeq, err := ts.Update([]byte("key1"), []byte("updated"), createSeq)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updateSeq != 2 {
		t.Fatalf("expected seqNum=2, got %d", updateSeq)
	}

	record, err := ts.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(record.Data) != "updated" {
		t.Fatalf("unexpected data: %s", record.Data)
	}
	if record.CreateSeqNum != 1 {
		t.Fatalf("createSeqNum should be preserved: %d", record.CreateSeqNum)
	}
	if record.UpdateSeqNum != 2 {
		t.Fatalf("updateSeqNum should be 2: %d", record.UpdateSeqNum)
	}
}

func TestUpdateNotFound(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	_, err := ts.Update([]byte("nonexistent"), []byte("data"), 0)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestOCCConflict(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	createSeq, err := ts.Create([]byte("key1"), []byte("data1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Update with correct expectedSeqNum succeeds.
	updateSeq, err := ts.Update([]byte("key1"), []byte("data2"), createSeq)
	if err != nil {
		t.Fatalf("first update: %v", err)
	}

	// Update with stale expectedSeqNum (createSeq) fails.
	_, err = ts.Update([]byte("key1"), []byte("data3"), createSeq)
	if err != ErrConflict {
		t.Fatalf("expected ErrConflict, got: %v", err)
	}

	// Update with correct expectedSeqNum succeeds.
	_, err = ts.Update([]byte("key1"), []byte("data3"), updateSeq)
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
}

func TestOCCConflictOnDelete(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	createSeq, err := ts.Create([]byte("key1"), []byte("data1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Update changes seqNum.
	updateSeq, err := ts.Update([]byte("key1"), []byte("data2"), createSeq)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	// Delete with stale seqNum fails.
	_, err = ts.Delete([]byte("key1"), createSeq)
	if err != ErrConflict {
		t.Fatalf("expected ErrConflict, got: %v", err)
	}

	// Delete with correct seqNum succeeds.
	_, err = ts.Delete([]byte("key1"), updateSeq)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestUnconditionalUpdate(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	if _, err := ts.Create([]byte("key1"), []byte("data1")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// expectedSeqNum=0 means unconditional — no OCC check.
	_, err := ts.Update([]byte("key1"), []byte("data2"), 0)
	if err != nil {
		t.Fatalf("unconditional update: %v", err)
	}
}

func TestUnconditionalDelete(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	if _, err := ts.Create([]byte("key1"), []byte("data1")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// expectedSeqNum=0 means unconditional — no OCC check.
	_, err := ts.Delete([]byte("key1"), 0)
	if err != nil {
		t.Fatalf("unconditional delete: %v", err)
	}

	_, err = ts.Get([]byte("key1"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	_, err := ts.Delete([]byte("nonexistent"), 0)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestDeleteAlreadyDeleted(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	seq, err := ts.Create([]byte("key1"), []byte("data1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := ts.Delete([]byte("key1"), seq); err != nil {
		t.Fatalf("first delete: %v", err)
	}

	// Second delete should fail — record is logically deleted.
	_, err = ts.Delete([]byte("key1"), 0)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for already-deleted, got: %v", err)
	}
}

func TestList(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	// Insert several records.
	for i := 0; i < 10; i++ {
		pk := fmt.Sprintf("key-%02d", i)
		data := fmt.Sprintf(`{"id":"%s","val":%d}`, pk, i)
		if _, err := ts.Create([]byte(pk), []byte(data)); err != nil {
			t.Fatalf("create %s: %v", pk, err)
		}
	}

	// Delete key-03 and key-07.
	r03, _ := ts.Get([]byte("key-03"))
	ts.Delete([]byte("key-03"), r03.UpdateSeqNum)
	r07, _ := ts.Get([]byte("key-07"))
	ts.Delete([]byte("key-07"), r07.UpdateSeqNum)

	// List all.
	entries, err := ts.List(nil, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 8 {
		t.Fatalf("expected 8 entries (10 - 2 deleted), got %d", len(entries))
	}

	// Verify deleted keys are excluded.
	for _, e := range entries {
		pk := string(e.PrimaryKey)
		if pk == "key-03" || pk == "key-07" {
			t.Fatalf("deleted key %s should not appear in list", pk)
		}
	}
}

func TestListWithBounds(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	for i := 0; i < 10; i++ {
		pk := fmt.Sprintf("key-%02d", i)
		if _, err := ts.Create([]byte(pk), []byte(pk)); err != nil {
			t.Fatalf("create %s: %v", pk, err)
		}
	}

	// List [key-03, key-07).
	entries, err := ts.List([]byte("key-03"), []byte("key-07"))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries [03,04,05,06], got %d", len(entries))
	}
	expected := []string{"key-03", "key-04", "key-05", "key-06"}
	for i, e := range entries {
		if string(e.PrimaryKey) != expected[i] {
			t.Fatalf("entry %d: expected %s, got %s", i, expected[i], e.PrimaryKey)
		}
	}
}

func TestRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "types", "events")
	reg := setupRegistry(t)
	opts := testTypeStoreOpts()

	// Phase 1: create records and close.
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := ts.Create([]byte("key1"), []byte("data1")); err != nil {
		t.Fatalf("create key1: %v", err)
	}
	seq2, err := ts.Create([]byte("key2"), []byte("data2"))
	if err != nil {
		t.Fatalf("create key2: %v", err)
	}
	if _, err := ts.Update([]byte("key2"), []byte("data2-updated"), seq2); err != nil {
		t.Fatalf("update key2: %v", err)
	}
	seq3, err := ts.Create([]byte("key3"), []byte("data3"))
	if err != nil {
		t.Fatalf("create key3: %v", err)
	}
	if _, err := ts.Delete([]byte("key3"), seq3); err != nil {
		t.Fatalf("delete key3: %v", err)
	}

	lastSeq := ts.LastSeqNum()
	ts.Close()

	// Phase 2: reopen and verify state recovered from WAL.
	ts2, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	// SeqNum counter should be restored.
	if ts2.LastSeqNum() != lastSeq {
		t.Fatalf("seqNum not restored: expected %d, got %d", lastSeq, ts2.LastSeqNum())
	}

	// key1 should exist with original data.
	r1, err := ts2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get key1: %v", err)
	}
	if string(r1.Data) != "data1" {
		t.Fatalf("key1 data: %s", r1.Data)
	}

	// key2 should have updated data.
	r2, err := ts2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("get key2: %v", err)
	}
	if string(r2.Data) != "data2-updated" {
		t.Fatalf("key2 data: %s", r2.Data)
	}
	if r2.CreateSeqNum != 2 {
		t.Fatalf("key2 createSeqNum: %d", r2.CreateSeqNum)
	}

	// key3 should be deleted.
	_, err = ts2.Get([]byte("key3"))
	if err != ErrNotFound {
		t.Fatalf("expected key3 deleted, got: %v", err)
	}

	// New writes after recovery should get correct seqNums.
	newSeq, err := ts2.Create([]byte("key4"), []byte("data4"))
	if err != nil {
		t.Fatalf("create key4: %v", err)
	}
	if newSeq != lastSeq+1 {
		t.Fatalf("post-recovery seqNum: expected %d, got %d", lastSeq+1, newSeq)
	}
}

func TestLastSeqNum(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	if ts.LastSeqNum() != 0 {
		t.Fatalf("initial seqNum should be 0, got %d", ts.LastSeqNum())
	}

	ts.Create([]byte("key1"), []byte("data1"))
	if ts.LastSeqNum() != 1 {
		t.Fatalf("after 1 write: expected 1, got %d", ts.LastSeqNum())
	}

	ts.Create([]byte("key2"), []byte("data2"))
	ts.Update([]byte("key1"), []byte("data1-v2"), 1)
	if ts.LastSeqNum() != 3 {
		t.Fatalf("after 3 writes: expected 3, got %d", ts.LastSeqNum())
	}
}

func TestClosedOperations(t *testing.T) {
	reg := setupRegistry(t)
	dir := filepath.Join(t.TempDir(), "types", "events")
	ts, err := OpenTypeStore(dir, "events", reg, testTypeStoreOpts())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ts.Close()

	if _, err := ts.Create([]byte("key"), []byte("data")); err != ErrClosed {
		t.Fatalf("Create on closed: expected ErrClosed, got %v", err)
	}
	if _, err := ts.Update([]byte("key"), []byte("data"), 0); err != ErrClosed {
		t.Fatalf("Update on closed: expected ErrClosed, got %v", err)
	}
	if _, err := ts.Delete([]byte("key"), 0); err != ErrClosed {
		t.Fatalf("Delete on closed: expected ErrClosed, got %v", err)
	}
	if _, err := ts.Get([]byte("key")); err != ErrClosed {
		t.Fatalf("Get on closed: expected ErrClosed, got %v", err)
	}
	if _, err := ts.List(nil, nil); err != ErrClosed {
		t.Fatalf("List on closed: expected ErrClosed, got %v", err)
	}
}

func TestManyKeys(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	n := 200
	for i := 0; i < n; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf("data-%04d", i)
		if _, err := ts.Create([]byte(pk), []byte(data)); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	// Verify all exist.
	for i := 0; i < n; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf("data-%04d", i)
		record, err := ts.Get([]byte(pk))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(record.Data) != data {
			t.Fatalf("key %d: expected %q, got %q", i, data, record.Data)
		}
	}

	// List should return all.
	entries, err := ts.List(nil, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("list: expected %d, got %d", n, len(entries))
	}
}

func TestStoredRecordMarshalRoundtrip(t *testing.T) {
	tests := []struct {
		name   string
		record *StoredRecord
	}{
		{
			name: "basic",
			record: &StoredRecord{
				Data:          []byte(`{"id":"123","name":"test"}`),
				SchemaVersion: "v1",
				CreateSeqNum:  42,
				UpdateSeqNum:  99,
			},
		},
		{
			name: "deleted",
			record: &StoredRecord{
				SchemaVersion: "v2",
				CreateSeqNum:  10,
				UpdateSeqNum:  20,
				IsDeleted:     true,
			},
		},
		{
			name: "empty data",
			record: &StoredRecord{
				SchemaVersion: "v1",
				CreateSeqNum:  1,
				UpdateSeqNum:  1,
			},
		},
		{
			name: "large data",
			record: &StoredRecord{
				Data:          bytes.Repeat([]byte("x"), 10000),
				SchemaVersion: "v3-long-version-name",
				CreateSeqNum:  1 << 40,
				UpdateSeqNum:  (1 << 40) + 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.record.Marshal()
			got, err := UnmarshalStoredRecord(buf)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !bytes.Equal(got.Data, tt.record.Data) {
				t.Fatalf("data mismatch")
			}
			if got.SchemaVersion != tt.record.SchemaVersion {
				t.Fatalf("schema version: %s vs %s", got.SchemaVersion, tt.record.SchemaVersion)
			}
			if got.CreateSeqNum != tt.record.CreateSeqNum {
				t.Fatalf("createSeqNum: %d vs %d", got.CreateSeqNum, tt.record.CreateSeqNum)
			}
			if got.UpdateSeqNum != tt.record.UpdateSeqNum {
				t.Fatalf("updateSeqNum: %d vs %d", got.UpdateSeqNum, tt.record.UpdateSeqNum)
			}
			if got.IsDeleted != tt.record.IsDeleted {
				t.Fatalf("isDeleted: %v vs %v", got.IsDeleted, tt.record.IsDeleted)
			}
		})
	}
}

func TestStoredRecordUnmarshalErrors(t *testing.T) {
	// Too short.
	if _, err := UnmarshalStoredRecord([]byte{1, 0}); err == nil {
		t.Fatal("expected error for short buffer")
	}

	// Unknown version.
	buf := make([]byte, recordHeaderSize)
	buf[0] = 99
	if _, err := UnmarshalStoredRecord(buf); err == nil {
		t.Fatal("expected error for unknown version")
	}

	// Truncated schema version.
	buf = make([]byte, recordHeaderSize)
	buf[0] = recordVersion
	buf[18] = 0
	buf[19] = 50 // schemaVersion length = 50, but buffer is only recordHeaderSize
	if _, err := UnmarshalStoredRecord(buf); err == nil {
		t.Fatal("expected error for truncated schema version")
	}
}

func TestConcurrentReads(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	// Write some data.
	for i := 0; i < 50; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		if _, err := ts.Create([]byte(pk), []byte(pk)); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	// Concurrent reads should not race.
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				pk := fmt.Sprintf("key-%04d", i)
				record, err := ts.Get([]byte(pk))
				if err != nil {
					t.Errorf("get %s: %v", pk, err)
					return
				}
				if string(record.Data) != pk {
					t.Errorf("data mismatch for %s", pk)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentWrites(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	// Concurrent creates with different keys should all succeed.
	var wg sync.WaitGroup
	errCh := make(chan error, 100)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				pk := fmt.Sprintf("g%d-key-%04d", goroutine, i)
				if _, err := ts.Create([]byte(pk), []byte(pk)); err != nil {
					errCh <- fmt.Errorf("create %s: %w", pk, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent write error: %v", err)
	}

	// Verify all 100 keys exist.
	entries, err := ts.List(nil, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 100 {
		t.Fatalf("expected 100 entries, got %d", len(entries))
	}
}

func TestStorageManager(t *testing.T) {
	baseDir := t.TempDir()
	reg := setupRegistry(t)
	opts := testTypeStoreOpts()

	sm := OpenStorageManager(baseDir, reg, opts)
	defer sm.Close()

	// First access creates the TypeStore.
	ts, err := sm.GetTypeStore("events")
	if err != nil {
		t.Fatalf("get type store: %v", err)
	}

	if _, err := ts.Create([]byte("key1"), []byte("data1")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Second access returns the same TypeStore.
	ts2, err := sm.GetTypeStore("events")
	if err != nil {
		t.Fatalf("get type store again: %v", err)
	}
	if ts2 != ts {
		t.Fatal("expected same TypeStore instance")
	}

	// Data persists across calls.
	record, err := ts2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(record.Data) != "data1" {
		t.Fatalf("unexpected data: %s", record.Data)
	}
}

func TestStorageManagerClose(t *testing.T) {
	baseDir := t.TempDir()
	reg := setupRegistry(t)
	opts := testTypeStoreOpts()

	sm := OpenStorageManager(baseDir, reg, opts)

	// Open a TypeStore.
	if _, err := sm.GetTypeStore("events"); err != nil {
		t.Fatalf("get type store: %v", err)
	}

	// Close manager.
	if err := sm.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Operations after close should fail.
	if _, err := sm.GetTypeStore("events"); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
}

func TestNoSchemaError(t *testing.T) {
	// Create a registry without setting a current version.
	dir := filepath.Join(t.TempDir(), "schema")
	reg, err := schema.Open(dir)
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	defer reg.Close()

	s := &schema.Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "id",
		Fields: map[string]schema.Field{
			"id": {Type: 1, Required: true},
		},
	}
	if err := reg.Register(s); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Don't set current.

	tsDir := filepath.Join(t.TempDir(), "types", "events")
	ts, err := OpenTypeStore(tsDir, "events", reg, testTypeStoreOpts())
	if err != nil {
		t.Fatalf("open type store: %v", err)
	}
	defer ts.Close()

	_, err = ts.Create([]byte("key1"), []byte("data1"))
	if err == nil {
		t.Fatal("expected error when no current schema")
	}
	// Should contain our sentinel error.
	if !bytes.Contains([]byte(err.Error()), []byte("no current schema")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSequentialUpdateChain(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	seq, err := ts.Create([]byte("key1"), []byte("v0"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Chain of 10 updates, each passing the previous seqNum.
	for i := 1; i <= 10; i++ {
		data := fmt.Sprintf("v%d", i)
		newSeq, err := ts.Update([]byte("key1"), []byte(data), seq)
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		seq = newSeq
	}

	record, err := ts.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(record.Data) != "v10" {
		t.Fatalf("expected v10, got %s", record.Data)
	}
	if record.CreateSeqNum != 1 {
		t.Fatalf("createSeqNum should be preserved at 1, got %d", record.CreateSeqNum)
	}
	if record.UpdateSeqNum != 11 { // 1 create + 10 updates
		t.Fatalf("updateSeqNum should be 11, got %d", record.UpdateSeqNum)
	}
}

// BenchmarkCreate measures Create throughput.
func BenchmarkCreate(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "schema")
	reg, err := schema.Open(dir)
	if err != nil {
		b.Fatalf("open registry: %v", err)
	}
	defer reg.Close()

	s := testSchema()
	reg.Register(s)
	reg.SetCurrent(s.Type, s.Version)

	tsDir := filepath.Join(b.TempDir(), "types", "events")
	ts, err := OpenTypeStore(tsDir, "events", reg, DefaultTypeStoreOptions())
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer ts.Close()

	data := []byte(`{"id":"x","name":"bench"}`)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pk := fmt.Sprintf("key-%08d", i)
		if _, err := ts.Create([]byte(pk), data); err != nil {
			b.Fatalf("create: %v", err)
		}
	}
}

// BenchmarkGet measures point-lookup throughput.
func BenchmarkGet(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "schema")
	reg, err := schema.Open(dir)
	if err != nil {
		b.Fatalf("open registry: %v", err)
	}
	defer reg.Close()

	s := testSchema()
	reg.Register(s)
	reg.SetCurrent(s.Type, s.Version)

	tsDir := filepath.Join(b.TempDir(), "types", "events")
	ts, err := OpenTypeStore(tsDir, "events", reg, DefaultTypeStoreOptions())
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer ts.Close()

	// Pre-populate.
	for i := 0; i < 1000; i++ {
		pk := fmt.Sprintf("key-%08d", i)
		ts.Create([]byte(pk), []byte(`{"id":"x"}`))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pk := fmt.Sprintf("key-%08d", i%1000)
		if _, err := ts.Get([]byte(pk)); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
