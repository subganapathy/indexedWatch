package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/subganapathy/indexedwatch/pkg/lsm"
)

const (
	// skSeparator separates the field value from the PK in SK keys.
	// \x00 is safe because field values are UTF-8 strings (no embedded \x00).
	skSeparator = '\x00'
)

// SecondaryIndex manages one LSM for a specific indexed field.
//
// SK key format: fieldValue + \x00 + pk
// SK value: 8-byte big-endian seqNum (the storage seqNum at the time of write)
//
// The \x00 separator enables prefix scanning:
//
//	SeekGE(fieldValue + \x00) and scan while key starts with fieldValue + \x00
//
// Different PKs with the same field value are adjacent in the SK LSM,
// making range scans efficient.
type SecondaryIndex struct {
	fieldName string
	db        *lsm.DB
}

// newSecondaryIndex creates a SecondaryIndex backed by the given LSM.
func newSecondaryIndex(fieldName string, db *lsm.DB) *SecondaryIndex {
	return &SecondaryIndex{
		fieldName: fieldName,
		db:        db,
	}
}

// FieldName returns the indexed field name.
func (si *SecondaryIndex) FieldName() string {
	return si.fieldName
}

// Update adds a new SK entry and optionally removes the old one.
// If oldFieldValue is empty, no removal is performed (i.e., this is a CREATE).
// If newFieldValue is empty, only removal is performed (i.e., this is a DELETE).
func (si *SecondaryIndex) Update(pk, oldFieldValue, newFieldValue []byte, seqNum uint64) error {
	// Remove old entry if field value changed.
	if len(oldFieldValue) > 0 && string(oldFieldValue) != string(newFieldValue) {
		oldKey := skKey(oldFieldValue, pk)
		if err := si.db.Delete(oldKey); err != nil {
			return fmt.Errorf("sk delete old %s: %w", si.fieldName, err)
		}
	}

	// Add new entry.
	if len(newFieldValue) > 0 {
		newKey := skKey(newFieldValue, pk)
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, seqNum)
		if err := si.db.Set(newKey, val); err != nil {
			return fmt.Errorf("sk set new %s: %w", si.fieldName, err)
		}
	}

	return nil
}

// Remove deletes the SK entry for the given field value and PK.
func (si *SecondaryIndex) Remove(pk, fieldValue []byte) error {
	if len(fieldValue) == 0 {
		return nil
	}
	key := skKey(fieldValue, pk)
	return si.db.Delete(key)
}

// Scan returns all primary keys that have the given field value.
// Results may include stale entries if the record was updated since the SK entry
// was written — callers should re-check against the Primary LSM.
func (si *SecondaryIndex) Scan(fieldValue []byte) ([][]byte, error) {
	prefix := skPrefix(fieldValue)

	it := si.db.NewIterator()
	defer it.Close()

	if !it.SeekGE(prefix) {
		return nil, nil
	}

	var pks [][]byte
	for it.Valid() {
		key := it.Key()
		if !hasPrefix(key, prefix) {
			break
		}

		// Extract PK from SK key: fieldValue + \x00 + pk
		pk := key[len(prefix):]
		pkCopy := make([]byte, len(pk))
		copy(pkCopy, pk)
		pks = append(pks, pkCopy)

		if !it.Next() {
			break
		}
	}

	return pks, nil
}

// Close closes the underlying LSM.
func (si *SecondaryIndex) Close() error {
	return si.db.Close()
}

// --- SK key helpers ---

// skKey builds the secondary index key: fieldValue + \x00 + pk.
func skKey(fieldValue, pk []byte) []byte {
	key := make([]byte, len(fieldValue)+1+len(pk))
	copy(key, fieldValue)
	key[len(fieldValue)] = skSeparator
	copy(key[len(fieldValue)+1:], pk)
	return key
}

// skPrefix builds the scan prefix: fieldValue + \x00.
func skPrefix(fieldValue []byte) []byte {
	prefix := make([]byte, len(fieldValue)+1)
	copy(prefix, fieldValue)
	prefix[len(fieldValue)] = skSeparator
	return prefix
}

// hasPrefix checks if b starts with prefix.
func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i, c := range prefix {
		if b[i] != c {
			return false
		}
	}
	return true
}
