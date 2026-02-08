package storage

import (
	"errors"
	"fmt"

	"github.com/subganapathy/indexedwatch/pkg/lsm"
)

// ErrIndexNotFound is returned when querying a field that has no secondary index.
var ErrIndexNotFound = errors.New("storage: secondary index not found")

// ListByField queries resources by a secondary index field value.
//
// The query flow:
//  1. Scan the SK LSM for all PKs matching the field value (prefix scan)
//  2. For each candidate PK, fetch the record from the Primary LSM
//  3. Re-check: verify the record's field value still matches (handles stale SK entries)
//  4. Filter out deleted records
//
// The re-check ensures correctness even if:
//   - The record was updated since the SK entry was written (field value changed)
//   - The record was deleted since the SK entry was written
//   - A crash occurred between Primary and SK LSM updates
func (ts *TypeStore) ListByField(fieldName string, fieldValue []byte) ([]ListEntry, error) {
	if ts.closed.Load() {
		return nil, ErrClosed
	}

	si := ts.skBuilder.getIndex(fieldName)
	if si == nil {
		return nil, fmt.Errorf("%w: %s", ErrIndexNotFound, fieldName)
	}

	// Step 1: Scan SK LSM for candidate PKs.
	candidatePKs, err := si.Scan(fieldValue)
	if err != nil {
		return nil, fmt.Errorf("storage: scan SK %s: %w", fieldName, err)
	}

	if len(candidatePKs) == 0 {
		return nil, nil
	}

	// Step 2+3: Fetch from Primary LSM and re-check.
	var results []ListEntry
	for _, pk := range candidatePKs {
		record, err := ts.getRecord(pk)
		if err != nil {
			if errors.Is(err, lsm.ErrNotFound) {
				continue // Record doesn't exist in Primary — stale SK entry
			}
			return nil, fmt.Errorf("storage: get %q: %w", pk, err)
		}

		// Filter deleted.
		if record.IsDeleted {
			continue
		}

		// Re-check: does the field still match?
		currentValue, ok := extractField(record.Data, fieldName)
		if !ok || currentValue != string(fieldValue) {
			continue // Stale SK entry — field value has changed
		}

		pkCopy := make([]byte, len(pk))
		copy(pkCopy, pk)
		results = append(results, ListEntry{
			PrimaryKey: pkCopy,
			Record:     record,
		})
	}

	return results, nil
}

// AddSecondaryIndex adds a secondary index for the given field and populates it
// by scanning the entire Primary LSM (re-indexing).
//
// This is called when the schema is updated to add a new secondary index field.
// Existing records are scanned and their field values indexed.
func (ts *TypeStore) AddSecondaryIndex(fieldName string) error {
	if ts.closed.Load() {
		return ErrClosed
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Check if index already exists.
	if ts.skBuilder.getIndex(fieldName) != nil {
		return nil // already indexed
	}

	// Open new SK LSM.
	si, err := ts.skBuilder.openIndex(fieldName)
	if err != nil {
		return fmt.Errorf("storage: open SK %s: %w", fieldName, err)
	}

	// Re-index: full scan of Primary LSM.
	if err := reindex(ts.db, si); err != nil {
		return fmt.Errorf("storage: reindex %s: %w", fieldName, err)
	}

	return nil
}
