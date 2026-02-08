package storage

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	storagev1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/storage/v1"
	"github.com/subganapathy/indexedwatch/pkg/lsm"
	"github.com/subganapathy/indexedwatch/pkg/schema"
	"github.com/subganapathy/indexedwatch/pkg/wal"
	"google.golang.org/protobuf/proto"
)

// walAppender is the subset of wal.WAL used by TypeStore.
// Extracted for testability.
type walAppender interface {
	Append(data []byte) (uint64, error)
	Sync() error
	Close() error
}

// WriteEvent is emitted after each successful write for watch notification.
type WriteEvent struct {
	SeqNum    uint64
	Operation storagev1.StorageOperation
	PrimaryKey []byte
	Data       []byte
}

// TypeStoreOptions configures a TypeStore.
type TypeStoreOptions struct {
	// LSMOptions configures the Primary LSM.
	LSMOptions lsm.Options
	// WALOptions configures the per-type resource WAL.
	WALOptions wal.Options
	// OnWrite is called after each successful write (WAL + LSM + SK applied).
	// Used by the watch system to feed events into the ring buffer.
	// Optional — nil means no notifications.
	OnWrite func(WriteEvent)
}

// DefaultTypeStoreOptions returns sensible defaults for production use.
func DefaultTypeStoreOptions() TypeStoreOptions {
	return TypeStoreOptions{
		WALOptions: wal.ResourceOptions(),
	}
}

// TypeStore manages the write-ahead log, Primary LSM, and Secondary LSMs
// for a single resource type.
//
// Write path (serialized by mu):
//  1. Run admission checkers (PK uniqueness, OCC, schema validation)
//  2. Build StorageWALEntry with operation, PK, data, seqNums
//  3. WAL Append + Sync (durable before LSM apply)
//  4. Apply to Primary LSM (immediately visible to reads)
//  5. Update Secondary LSMs synchronously (strong consistency)
//
// Read path (lock-free, reads go directly to LSM):
//   - Get: point lookup by PK via Primary LSM
//   - List: range scan with optional prefix bounds via Primary LSM
//   - ListByField: secondary index scan → Primary re-check (see query.go)
//
// Recovery: replay all WAL entries into the Primary LSM (see apply.go),
// then re-index all secondary indexes from the Primary LSM.
type TypeStore struct {
	typeName  string
	dir       string
	w         walAppender
	db        *lsm.DB
	registry  schema.Registry
	checkers  []AdmissionChecker
	skBuilder *skBuilder
	onWrite   func(WriteEvent)

	// seqNum is the storage-level sequence number, separate from LSM's internal seqNum.
	// Monotonically increasing. Stored in WAL entries and StoredRecords for OCC.
	seqNum atomic.Uint64

	// mu serializes write operations. Reads are lock-free.
	// Serialization is required for OCC correctness: read-check-write must be atomic.
	mu     sync.Mutex
	closed atomic.Bool
}

// OpenTypeStore opens or creates a TypeStore for the given resource type.
// The directory structure is:
//
//	dir/
//	  wal/   — per-type resource WAL
//	  lsm/   — Primary LSM
//	  sk/    — Secondary LSMs (one per indexed field)
//	    user_id/
//	    metadata_region/
func OpenTypeStore(dir, typeName string, reg schema.Registry, opts TypeStoreOptions) (*TypeStore, error) {
	if opts.WALOptions == (wal.Options{}) {
		opts.WALOptions = wal.ResourceOptions()
	}

	walDir := filepath.Join(dir, "wal")
	lsmDir := filepath.Join(dir, "lsm")
	skDir := filepath.Join(dir, "sk")

	w, err := wal.Open(walDir, opts.WALOptions)
	if err != nil {
		return nil, fmt.Errorf("storage: open WAL for %s: %w", typeName, err)
	}

	db, err := lsm.Open(lsmDir, opts.LSMOptions)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("storage: open LSM for %s: %w", typeName, err)
	}

	skb := newSKBuilder(skDir, opts.LSMOptions)

	ts := &TypeStore{
		typeName:  typeName,
		dir:       dir,
		w:         w,
		db:        db,
		registry:  reg,
		checkers:  defaultCheckers(),
		skBuilder: skb,
		onWrite:   opts.OnWrite,
	}

	// Recover state: load snapshot (if available) + replay WAL entries.
	maxSeqNum, err := recoverWithSnapshot(w, db, dir)
	if err != nil {
		skb.close()
		db.Close()
		w.Close()
		return nil, fmt.Errorf("storage: recover %s: %w", typeName, err)
	}
	ts.seqNum.Store(maxSeqNum)

	// Open secondary indexes from the current schema and re-index.
	if err := ts.initSecondaryIndexes(); err != nil {
		skb.close()
		db.Close()
		w.Close()
		return nil, fmt.Errorf("storage: init secondary indexes for %s: %w", typeName, err)
	}

	return ts, nil
}

// Create inserts a new resource. Returns the assigned storage seqNum.
// Fails with ErrAlreadyExists if the PK already exists (and is not deleted).
func (ts *TypeStore) Create(pk, data []byte) (uint64, error) {
	if ts.closed.Load() {
		return 0, ErrClosed
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Read existing record for admission.
	existing := ts.getRecordOrNil(pk)

	// Run admission.
	req := &WriteRequest{
		Operation:  storagev1.StorageOperation_STORAGE_OPERATION_CREATE,
		PrimaryKey: pk,
		Data:       data,
		Existing:   existing,
	}
	if err := runAdmission(ts.checkers, req); err != nil {
		return 0, err
	}

	seqNum := ts.seqNum.Add(1)

	// Build and persist WAL entry.
	entry := &storagev1.StorageWALEntry{
		Operation:    storagev1.StorageOperation_STORAGE_OPERATION_CREATE,
		PrimaryKey:   pk,
		Data:         data,
		CreateSeqNum: seqNum,
		UpdateSeqNum: seqNum,
	}
	if err := ts.writeWAL(entry); err != nil {
		return 0, err
	}

	// Apply to LSM.
	record := &StoredRecord{
		Data:         data,
		CreateSeqNum: seqNum,
		UpdateSeqNum: seqNum,
	}
	if err := ts.db.Set(pk, record.Marshal()); err != nil {
		return 0, fmt.Errorf("storage: LSM set: %w", err)
	}

	// Update secondary indexes.
	if err := ts.skBuilder.processWrite(
		storagev1.StorageOperation_STORAGE_OPERATION_CREATE,
		pk, nil, data, seqNum,
	); err != nil {
		return 0, fmt.Errorf("storage: SK update: %w", err)
	}

	// Notify watchers.
	ts.notifyWrite(seqNum, storagev1.StorageOperation_STORAGE_OPERATION_CREATE, pk, data)

	return seqNum, nil
}

// Update modifies an existing resource. Returns the assigned storage seqNum.
// The expectedSeqNum must match the record's current UpdateSeqNum (OCC).
// Pass expectedSeqNum=0 for unconditional update (no OCC check).
func (ts *TypeStore) Update(pk, data []byte, expectedSeqNum uint64) (uint64, error) {
	if ts.closed.Load() {
		return 0, ErrClosed
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	existing := ts.getRecordOrNil(pk)

	req := &WriteRequest{
		Operation:      storagev1.StorageOperation_STORAGE_OPERATION_UPDATE,
		PrimaryKey:     pk,
		Data:           data,
		ExpectedSeqNum: expectedSeqNum,
		Existing:       existing,
	}
	if err := runAdmission(ts.checkers, req); err != nil {
		return 0, err
	}

	seqNum := ts.seqNum.Add(1)

	entry := &storagev1.StorageWALEntry{
		Operation:      storagev1.StorageOperation_STORAGE_OPERATION_UPDATE,
		PrimaryKey:     pk,
		Data:           data,
		ExpectedSeqNum: expectedSeqNum,
		CreateSeqNum:   existing.CreateSeqNum,
		UpdateSeqNum:   seqNum,
	}
	if err := ts.writeWAL(entry); err != nil {
		return 0, err
	}

	record := &StoredRecord{
		Data:         data,
		CreateSeqNum: existing.CreateSeqNum,
		UpdateSeqNum: seqNum,
	}
	if err := ts.db.Set(pk, record.Marshal()); err != nil {
		return 0, fmt.Errorf("storage: LSM set: %w", err)
	}

	// Update secondary indexes (pass old data for SK entry removal).
	if err := ts.skBuilder.processWrite(
		storagev1.StorageOperation_STORAGE_OPERATION_UPDATE,
		pk, existing.Data, data, seqNum,
	); err != nil {
		return 0, fmt.Errorf("storage: SK update: %w", err)
	}

	// Notify watchers.
	ts.notifyWrite(seqNum, storagev1.StorageOperation_STORAGE_OPERATION_UPDATE, pk, data)

	return seqNum, nil
}

// Delete removes an existing resource (logical delete). Returns the assigned storage seqNum.
// The expectedSeqNum must match the record's current UpdateSeqNum (OCC).
// Pass expectedSeqNum=0 for unconditional delete.
func (ts *TypeStore) Delete(pk []byte, expectedSeqNum uint64) (uint64, error) {
	if ts.closed.Load() {
		return 0, ErrClosed
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	existing := ts.getRecordOrNil(pk)

	req := &WriteRequest{
		Operation:      storagev1.StorageOperation_STORAGE_OPERATION_DELETE,
		PrimaryKey:     pk,
		ExpectedSeqNum: expectedSeqNum,
		Existing:       existing,
	}
	if err := runAdmission(ts.checkers, req); err != nil {
		return 0, err
	}

	seqNum := ts.seqNum.Add(1)

	entry := &storagev1.StorageWALEntry{
		Operation:      storagev1.StorageOperation_STORAGE_OPERATION_DELETE,
		PrimaryKey:     pk,
		ExpectedSeqNum: expectedSeqNum,
		CreateSeqNum:   existing.CreateSeqNum,
		UpdateSeqNum:   seqNum,
	}
	if err := ts.writeWAL(entry); err != nil {
		return 0, err
	}

	// Logical delete: store tombstone with IsDeleted=true.
	record := &StoredRecord{
		IsDeleted:    true,
		CreateSeqNum: existing.CreateSeqNum,
		UpdateSeqNum: seqNum,
	}
	if err := ts.db.Set(pk, record.Marshal()); err != nil {
		return 0, fmt.Errorf("storage: LSM set: %w", err)
	}

	// Update secondary indexes (remove old SK entries).
	if err := ts.skBuilder.processWrite(
		storagev1.StorageOperation_STORAGE_OPERATION_DELETE,
		pk, existing.Data, nil, seqNum,
	); err != nil {
		return 0, fmt.Errorf("storage: SK update: %w", err)
	}

	// Notify watchers.
	ts.notifyWrite(seqNum, storagev1.StorageOperation_STORAGE_OPERATION_DELETE, pk, nil)

	return seqNum, nil
}

// Get retrieves a resource by primary key.
// Returns ErrNotFound if the resource does not exist or has been deleted.
func (ts *TypeStore) Get(pk []byte) (*StoredRecord, error) {
	if ts.closed.Load() {
		return nil, ErrClosed
	}

	record, err := ts.getRecord(pk)
	if err != nil {
		if errors.Is(err, lsm.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if record.IsDeleted {
		return nil, ErrNotFound
	}
	return record, nil
}

// ListEntry is a primary key + record pair returned by List.
type ListEntry struct {
	PrimaryKey []byte
	Record     *StoredRecord
}

// List returns all non-deleted resources whose PK is in [startKey, endKey).
// Pass nil for startKey to start from the beginning.
// Pass nil for endKey to scan to the end.
func (ts *TypeStore) List(startKey, endKey []byte) ([]ListEntry, error) {
	if ts.closed.Load() {
		return nil, ErrClosed
	}

	it := ts.db.NewIterator()
	defer it.Close()

	var positioned bool
	if len(startKey) > 0 {
		positioned = it.SeekGE(startKey)
	} else {
		positioned = it.First()
	}

	var entries []ListEntry
	for positioned && it.Valid() {
		key := it.Key()
		if len(endKey) > 0 && bytes.Compare(key, endKey) >= 0 {
			break
		}

		record, err := UnmarshalStoredRecord(it.Value())
		if err != nil {
			return nil, fmt.Errorf("storage: unmarshal record at %q: %w", key, err)
		}

		if !record.IsDeleted {
			pk := make([]byte, len(key))
			copy(pk, key)
			entries = append(entries, ListEntry{
				PrimaryKey: pk,
				Record:     record,
			})
		}

		positioned = it.Next()
	}

	return entries, nil
}

// LastSeqNum returns the highest storage sequence number assigned.
func (ts *TypeStore) LastSeqNum() uint64 {
	return ts.seqNum.Load()
}

// TypeName returns the resource type name.
func (ts *TypeStore) TypeName() string {
	return ts.typeName
}

// Close shuts down the TypeStore, closing the WAL, Primary LSM, and all Secondary LSMs.
func (ts *TypeStore) Close() error {
	if !ts.closed.CompareAndSwap(false, true) {
		return nil
	}

	var firstErr error
	if err := ts.skBuilder.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := ts.db.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := ts.w.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// --- Internal helpers ---

// currentSchema returns the schema definition for this type.
func (ts *TypeStore) currentSchema() (*schema.Schema, error) {
	s, err := ts.registry.Get(ts.typeName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrNoSchema, ts.typeName, err)
	}
	return s, nil
}

// getRecord reads and unmarshals a record from the LSM.
func (ts *TypeStore) getRecord(pk []byte) (*StoredRecord, error) {
	val, err := ts.db.Get(pk)
	if err != nil {
		return nil, err
	}
	return UnmarshalStoredRecord(val)
}

// getRecordOrNil reads a record, returning nil if not found.
func (ts *TypeStore) getRecordOrNil(pk []byte) *StoredRecord {
	record, err := ts.getRecord(pk)
	if err != nil {
		return nil
	}
	return record
}

// notifyWrite calls the OnWrite callback if set.
func (ts *TypeStore) notifyWrite(seqNum uint64, op storagev1.StorageOperation, pk, data []byte) {
	if ts.onWrite != nil {
		ts.onWrite(WriteEvent{
			SeqNum:     seqNum,
			Operation:  op,
			PrimaryKey: pk,
			Data:       data,
		})
	}
}

// SnapshotForWatch returns all non-deleted resources at the current point-in-time,
// suitable for the snapshot phase of a watch stream.
// Returns the seqNum at which the snapshot was taken and the list of resources.
func (ts *TypeStore) SnapshotForWatch() (seqNum uint64, pks [][]byte, records []*StoredRecord, err error) {
	if ts.closed.Load() {
		return 0, nil, nil, ErrClosed
	}

	ts.mu.Lock()
	seqNum = ts.seqNum.Load()
	it := ts.db.NewIterator()
	ts.mu.Unlock()
	defer it.Close()

	if it.First() {
		for it.Valid() {
			record, rerr := UnmarshalStoredRecord(it.Value())
			if rerr != nil {
				return 0, nil, nil, fmt.Errorf("storage: unmarshal snapshot record: %w", rerr)
			}
			if !record.IsDeleted {
				pk := make([]byte, len(it.Key()))
				copy(pk, it.Key())
				pks = append(pks, pk)
				records = append(records, record)
			}
			if !it.Next() {
				break
			}
		}
	}

	return seqNum, pks, records, nil
}

// writeWAL marshals and persists a WAL entry, then syncs.
func (ts *TypeStore) writeWAL(entry *storagev1.StorageWALEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("storage: marshal WAL entry: %w", err)
	}

	if _, err := ts.w.Append(data); err != nil {
		return fmt.Errorf("storage: WAL append: %w", err)
	}
	if err := ts.w.Sync(); err != nil {
		return fmt.Errorf("storage: WAL sync: %w", err)
	}

	return nil
}

// initSecondaryIndexes opens SK LSMs for all secondary indexes defined in
// the current schema and re-indexes them from the Primary LSM.
func (ts *TypeStore) initSecondaryIndexes() error {
	// If the type has no schema registered, skip — no indexes to create.
	currentSchema, err := ts.registry.Get(ts.typeName)
	if err != nil {
		return nil // no current schema, no indexes
	}

	for _, fieldName := range currentSchema.SecondaryIndexes {
		si, err := ts.skBuilder.openIndex(fieldName)
		if err != nil {
			return fmt.Errorf("open SK %s: %w", fieldName, err)
		}

		// Re-index from Primary LSM to ensure SK is consistent.
		if err := reindex(ts.db, si); err != nil {
			return fmt.Errorf("reindex %s: %w", fieldName, err)
		}
	}

	return nil
}
