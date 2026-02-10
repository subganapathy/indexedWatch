package storage

import (
	"fmt"

	storagev1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/storage/v1"
	"github.com/subganapathy/indexedwatch/pkg/lsm"
	"google.golang.org/protobuf/proto"
)

// recover replays all WAL entries into the LSM to restore state.
//
// On normal startup, the LSM recovers from its MANIFEST + SSTables, which
// contain data that was flushed before the last shutdown. WAL entries that
// were applied to the memtable but not yet flushed are lost. Replaying all
// WAL entries ensures those "in-flight" writes are re-applied.
//
// For entries already in SSTables, the replay creates new LSM internal versions
// (with new LSM seqNums) containing the same StoredRecord data. This is
// redundant but harmless — MVCC resolution picks the newest, and the data
// is identical. PR 11 (snapshots) optimizes this by pruning old WAL segments.
func recover(w walAppender, db *lsm.DB, seqNum *uint64) error {
	type reader interface {
		ReadAll() ([][]byte, error)
	}
	r, ok := w.(reader)
	if !ok {
		return fmt.Errorf("storage: WAL does not support ReadAll")
	}

	entries, err := r.ReadAll()
	if err != nil {
		return fmt.Errorf("storage: WAL ReadAll: %w", err)
	}

	var maxSeqNum uint64
	for i, data := range entries {
		entry := &storagev1.StorageWALEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return fmt.Errorf("storage: unmarshal entry %d: %w", i, err)
		}

		if err := applyEntry(db, entry); err != nil {
			return fmt.Errorf("storage: apply entry %d: %w", i, err)
		}

		if entry.UpdateSeqNum > maxSeqNum {
			maxSeqNum = entry.UpdateSeqNum
		}
	}

	*seqNum = maxSeqNum
	return nil
}

// applyEntry writes a single WAL entry to the LSM.
// For CREATE and UPDATE, the record is stored with its full data.
// For DELETE, a logical tombstone (IsDeleted=true) is stored.
func applyEntry(db *lsm.DB, entry *storagev1.StorageWALEntry) error {
	switch entry.Operation {
	case storagev1.StorageOperation_STORAGE_OPERATION_CREATE,
		storagev1.StorageOperation_STORAGE_OPERATION_UPDATE:
		record := &StoredRecord{
			Data:         entry.Data,
			CreateSeqNum: entry.CreateSeqNum,
			UpdateSeqNum: entry.UpdateSeqNum,
		}
		return db.Set(entry.PrimaryKey, record.Marshal())

	case storagev1.StorageOperation_STORAGE_OPERATION_DELETE:
		record := &StoredRecord{
			IsDeleted:    true,
			CreateSeqNum: entry.CreateSeqNum,
			UpdateSeqNum: entry.UpdateSeqNum,
		}
		return db.Set(entry.PrimaryKey, record.Marshal())

	default:
		return fmt.Errorf("unknown operation: %d", entry.Operation)
	}
}
