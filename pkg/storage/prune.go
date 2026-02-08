package storage

import (
	"fmt"

	storagev1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/storage/v1"
	"github.com/subganapathy/indexedwatch/pkg/lsm"
	"github.com/subganapathy/indexedwatch/pkg/wal/walpb"
	"google.golang.org/protobuf/proto"
)

// TakeSnapshot takes a point-in-time snapshot of the Primary LSM, writes it
// to a file, records a WAL snapshot marker, and prunes old WAL segments.
//
// This is the main entry point for periodic checkpointing. The flow:
//  1. Capture current storage seqNum and LSM iterator (under write lock)
//  2. Write all non-deleted records to a snapshot file
//  3. Write SnapshotType marker to WAL
//  4. Write snapshot.meta file
//  5. Prune old WAL segments and snapshot files
func (ts *TypeStore) TakeSnapshot() (*SnapshotMeta, error) {
	if ts.closed.Load() {
		return nil, ErrClosed
	}

	// Step 1: capture consistent point-in-time view.
	ts.mu.Lock()
	seqNum := ts.seqNum.Load()
	it := ts.db.NewIterator()
	ts.mu.Unlock()
	defer it.Close()

	// Step 2: write snapshot file.
	snapPath, err := takeSnapshot(it, ts.dir, seqNum)
	if err != nil {
		return nil, fmt.Errorf("storage: take snapshot: %w", err)
	}

	// Step 3: write WAL snapshot marker.
	walSegSeq := ts.walSegmentSeq()
	snapMeta := &SnapshotMeta{
		SeqNum:        seqNum,
		WALSegmentSeq: walSegSeq,
		Path:          snapPath,
	}

	if err := ts.saveWALSnapshot(seqNum, snapPath); err != nil {
		return nil, fmt.Errorf("storage: WAL snapshot marker: %w", err)
	}

	// Step 4: write snapshot metadata file.
	if err := writeSnapshotMeta(ts.dir, snapMeta); err != nil {
		return nil, fmt.Errorf("storage: write snapshot meta: %w", err)
	}

	// Step 5: prune old WAL segments and snapshot files.
	ts.pruneWAL(walSegSeq)
	removeOldSnapshots(ts.dir, snapPath)

	return snapMeta, nil
}

// walSegmentSeq returns the current WAL segment sequence number.
func (ts *TypeStore) walSegmentSeq() uint64 {
	type segSeqer interface {
		SegmentSeq() uint64
	}
	if ss, ok := ts.w.(segSeqer); ok {
		return ss.SegmentSeq()
	}
	return 0
}

// saveWALSnapshot writes a SnapshotType marker to the WAL.
func (ts *TypeStore) saveWALSnapshot(seqNum uint64, path string) error {
	type snapshotSaver interface {
		SaveSnapshot(snap *walpb.Snapshot) error
	}
	ss, ok := ts.w.(snapshotSaver)
	if !ok {
		return nil // WAL doesn't support snapshots (e.g., test mock)
	}
	return ss.SaveSnapshot(&walpb.Snapshot{
		Offset:   seqNum,
		Metadata: []byte(path),
	})
}

// pruneWAL deletes WAL segments older than the given segment sequence number.
func (ts *TypeStore) pruneWAL(keepSegSeq uint64) {
	type pruner interface {
		PruneBefore(keepSeq uint64) (int, error)
	}
	if p, ok := ts.w.(pruner); ok {
		p.PruneBefore(keepSegSeq)
	}
}

// recoverWithSnapshot loads a snapshot file (if available) and replays
// WAL entries after the snapshot's seqNum for faster recovery.
// Falls back to full WAL replay if no snapshot exists or if it's corrupted.
func recoverWithSnapshot(w walAppender, lsmDB *lsm.DB, dir string) (uint64, error) {
	meta := readSnapshotMeta(dir)

	if meta != nil {
		// Load snapshot file into LSM.
		snapSeqNum, err := loadSnapshot(meta.Path, lsmDB)
		if err == nil {
			// Replay only WAL entries after the snapshot.
			maxSeqNum, err := replayWALAfter(w, lsmDB, snapSeqNum)
			if err != nil {
				return 0, err
			}
			if maxSeqNum < snapSeqNum {
				maxSeqNum = snapSeqNum
			}
			return maxSeqNum, nil
		}
		// Snapshot corrupted — fall through to full replay.
	}

	// No snapshot or corrupted — full WAL replay.
	var maxSeqNum uint64
	if err := recover(w, lsmDB, &maxSeqNum); err != nil {
		return 0, err
	}
	return maxSeqNum, nil
}

// replayWALAfter replays WAL entries with update_seq_num > afterSeqNum.
func replayWALAfter(w walAppender, db *lsm.DB, afterSeqNum uint64) (uint64, error) {
	type reader interface {
		ReadAll() ([][]byte, error)
	}
	r, ok := w.(reader)
	if !ok {
		return 0, fmt.Errorf("storage: WAL does not support ReadAll")
	}

	entries, err := r.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("storage: WAL ReadAll: %w", err)
	}

	var maxSeqNum uint64
	for _, data := range entries {
		entry := &storagev1.StorageWALEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			continue // skip corrupted entries during recovery
		}

		// Skip entries already covered by the snapshot.
		if entry.UpdateSeqNum <= afterSeqNum {
			continue
		}

		if err := applyEntry(db, entry); err != nil {
			return 0, err
		}

		if entry.UpdateSeqNum > maxSeqNum {
			maxSeqNum = entry.UpdateSeqNum
		}
	}

	return maxSeqNum, nil
}
