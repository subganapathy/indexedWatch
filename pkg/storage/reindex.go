package storage

import (
	"fmt"

	"github.com/subganapathy/indexedwatch/pkg/lsm"
)

// reindex performs a full scan of the Primary LSM to build a secondary index
// from scratch. This is called when a new secondary index is added to the schema.
//
// The process:
//  1. Create a new SecondaryIndex (opens or reuses the LSM)
//  2. Iterate over all live records in the Primary LSM
//  3. For each record, extract the field value and write an SK entry
//
// Records that are logically deleted (IsDeleted=true) are skipped.
// After re-indexing, the secondary index is consistent with the Primary LSM's
// current state.
func reindex(primaryDB *lsm.DB, si *SecondaryIndex) error {
	it := primaryDB.NewIterator()
	defer it.Close()

	if !it.First() {
		return nil // empty database
	}

	var count int
	for it.Valid() {
		pk := it.Key()
		val := it.Value()

		record, err := UnmarshalStoredRecord(val)
		if err != nil {
			return fmt.Errorf("reindex: unmarshal %q: %w", pk, err)
		}

		if record.IsDeleted {
			if !it.Next() {
				break
			}
			continue
		}

		// Extract the field value from the record's data.
		fieldValue, ok := extractField(record.Data, si.fieldName)
		if ok && fieldValue != "" {
			if err := si.Update(pk, nil, []byte(fieldValue), record.UpdateSeqNum); err != nil {
				return fmt.Errorf("reindex: update SK for %q: %w", pk, err)
			}
			count++
		}

		if !it.Next() {
			break
		}
	}

	return nil
}
