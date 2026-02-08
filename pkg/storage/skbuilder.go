package storage

import (
	"fmt"
	"path/filepath"

	storagev1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/storage/v1"
	"github.com/subganapathy/indexedwatch/pkg/lsm"
)

// skBuilder coordinates updates to all secondary indexes for a TypeStore.
//
// On each write to the Primary LSM, the TypeStore calls processWrite to
// update all secondary indexes synchronously. This ensures strong consistency
// between Primary and Secondary LSMs — no eventual consistency lag.
//
// For each indexed field, the builder:
//   - Extracts the field value from the old and new data (JSON)
//   - Removes the old SK entry if the field value changed
//   - Adds the new SK entry with the current seqNum
type skBuilder struct {
	indexes map[string]*SecondaryIndex // fieldName → SecondaryIndex
	skDir   string                     // base directory for SK LSMs
	lsmOpts lsm.Options               // LSM options for SK databases
}

// newSKBuilder creates a builder that manages secondary indexes.
func newSKBuilder(skDir string, lsmOpts lsm.Options) *skBuilder {
	return &skBuilder{
		indexes: make(map[string]*SecondaryIndex),
		skDir:   skDir,
		lsmOpts: lsmOpts,
	}
}

// openIndex opens or creates a SecondaryIndex for the given field.
func (b *skBuilder) openIndex(fieldName string) (*SecondaryIndex, error) {
	if si, ok := b.indexes[fieldName]; ok {
		return si, nil
	}

	// Each secondary index gets its own LSM directory.
	dir := filepath.Join(b.skDir, sanitizeFieldName(fieldName))
	db, err := lsm.Open(dir, b.lsmOpts)
	if err != nil {
		return nil, fmt.Errorf("sk open %s: %w", fieldName, err)
	}

	si := newSecondaryIndex(fieldName, db)
	b.indexes[fieldName] = si
	return si, nil
}

// getIndex returns an existing SecondaryIndex, or nil if not open.
func (b *skBuilder) getIndex(fieldName string) *SecondaryIndex {
	return b.indexes[fieldName]
}

// processWrite updates all secondary indexes for a single write operation.
// oldData is the previous record's data (nil for CREATE, non-nil for UPDATE/DELETE).
// newData is the new record's data (nil for DELETE).
func (b *skBuilder) processWrite(
	op storagev1.StorageOperation,
	pk []byte,
	oldData []byte,
	newData []byte,
	seqNum uint64,
) error {
	for fieldName, si := range b.indexes {
		var oldFieldValue, newFieldValue []byte

		// Extract old field value (for UPDATE/DELETE).
		if len(oldData) > 0 {
			if val, ok := extractField(oldData, fieldName); ok {
				oldFieldValue = []byte(val)
			}
		}

		// Extract new field value (for CREATE/UPDATE).
		if len(newData) > 0 {
			if val, ok := extractField(newData, fieldName); ok {
				newFieldValue = []byte(val)
			}
		}

		switch op {
		case storagev1.StorageOperation_STORAGE_OPERATION_CREATE:
			if err := si.Update(pk, nil, newFieldValue, seqNum); err != nil {
				return err
			}

		case storagev1.StorageOperation_STORAGE_OPERATION_UPDATE:
			if err := si.Update(pk, oldFieldValue, newFieldValue, seqNum); err != nil {
				return err
			}

		case storagev1.StorageOperation_STORAGE_OPERATION_DELETE:
			if err := si.Remove(pk, oldFieldValue); err != nil {
				return err
			}
		}
	}

	return nil
}

// close shuts down all secondary index LSMs.
func (b *skBuilder) close() error {
	var firstErr error
	for _, si := range b.indexes {
		if err := si.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	b.indexes = nil
	return firstErr
}

// sanitizeFieldName converts a field path to a safe directory name.
// Replaces dots with underscores (e.g., "metadata.region" → "metadata_region").
func sanitizeFieldName(fieldName string) string {
	result := make([]byte, len(fieldName))
	for i := 0; i < len(fieldName); i++ {
		if fieldName[i] == '.' {
			result[i] = '_'
		} else {
			result[i] = fieldName[i]
		}
	}
	return string(result)
}
