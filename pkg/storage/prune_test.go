package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/subganapathy/indexedwatch/pkg/wal"
)

func TestWALPruneBefore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")

	// Use small segments to force rotation.
	w, err := wal.Open(dir, wal.Options{
		SegmentSize: 256, // Very small, forces rotation quickly
		BufferSize:  64,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write enough data to create multiple segments.
	data := make([]byte, 100)
	for i := 0; i < 20; i++ {
		if _, err := w.Append(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if err := w.Sync(); err != nil {
			t.Fatalf("sync %d: %v", i, err)
		}
	}

	// Count segments before pruning.
	entries, _ := os.ReadDir(dir)
	segmentCount := 0
	for _, e := range entries {
		if !e.IsDir() {
			segmentCount++
		}
	}
	if segmentCount < 2 {
		t.Fatalf("expected at least 2 segments, got %d", segmentCount)
	}

	// Get current segment seq.
	currentSeq := w.SegmentSeq()
	t.Logf("segments: %d, current seq: %d", segmentCount, currentSeq)

	// Prune all segments before the current one.
	deleted, err := w.PruneBefore(currentSeq)
	if err != nil {
		t.Fatalf("PruneBefore: %v", err)
	}
	if deleted == 0 {
		t.Fatal("expected to delete at least 1 segment")
	}

	// Count segments after pruning.
	entries, _ = os.ReadDir(dir)
	afterCount := 0
	for _, e := range entries {
		if !e.IsDir() {
			afterCount++
		}
	}
	// Should have fewer segments.
	if afterCount >= segmentCount {
		t.Fatalf("expected fewer segments: before=%d, after=%d", segmentCount, afterCount)
	}
	// Active segment should still exist.
	if afterCount < 1 {
		t.Fatal("active segment was deleted")
	}

	// WAL should still be usable.
	if _, err := w.Append([]byte("after-prune")); err != nil {
		t.Fatalf("append after prune: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync after prune: %v", err)
	}

	w.Close()
}

func TestWALPruneBeforeKeepsCurrent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")

	w, err := wal.Open(dir, wal.DefaultOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	// Only one segment (seq=0).
	w.Append([]byte("data"))
	w.Sync()

	// Prune with keepSeq=999 — should not delete the active segment.
	deleted, err := w.PruneBefore(999)
	if err != nil {
		t.Fatalf("PruneBefore: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("should not delete active segment: deleted=%d", deleted)
	}
}

func TestSnapshotAndPruneIntegration(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "types", "events")
	reg := setupRegistry(t)
	opts := TypeStoreOptions{
		LSMOptions: testTypeStoreOpts().LSMOptions,
		WALOptions: wal.Options{
			SegmentSize: 256, // Small segments for testing
			BufferSize:  64,
		},
	}

	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write enough data to create multiple WAL segments.
	for i := 0; i < 50; i++ {
		pk := make([]byte, 50)
		copy(pk, []byte("key-"))
		pk[4] = byte('0' + i/10)
		pk[5] = byte('0' + i%10)
		data := make([]byte, 100)
		copy(data, []byte(`{"id":"x","name":"y"}`))
		ts.Create(pk[:6], data)
	}

	// Count WAL segments before snapshot.
	walDir := filepath.Join(dir, "wal")
	entries, _ := os.ReadDir(walDir)
	segsBefore := 0
	for _, e := range entries {
		if !e.IsDir() {
			segsBefore++
		}
	}

	// Take snapshot + prune.
	_, err = ts.TakeSnapshot()
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Count WAL segments after snapshot.
	entries, _ = os.ReadDir(walDir)
	segsAfter := 0
	for _, e := range entries {
		if !e.IsDir() {
			segsAfter++
		}
	}

	t.Logf("WAL segments: before=%d, after=%d", segsBefore, segsAfter)

	// Should have pruned some segments (if there were more than 1).
	if segsBefore > 1 && segsAfter >= segsBefore {
		t.Fatalf("expected pruning to reduce segments: before=%d, after=%d", segsBefore, segsAfter)
	}

	ts.Close()

	// Reopen — should recover from snapshot.
	ts2, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	// All data should be present.
	allEntries, err := ts2.List(nil, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(allEntries) != 50 {
		t.Fatalf("expected 50 entries, got %d", len(allEntries))
	}
}
