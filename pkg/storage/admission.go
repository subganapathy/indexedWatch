package storage

import (
	storagev1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/storage/v1"
)

// AdmissionChecker validates a write operation before it is committed to the WAL.
// Checkers run in sequence; the first error aborts the write.
type AdmissionChecker interface {
	Check(req *WriteRequest) error
}

// WriteRequest carries all context needed for admission checks.
type WriteRequest struct {
	// Operation is the write operation type (CREATE, UPDATE, DELETE).
	Operation storagev1.StorageOperation

	// PrimaryKey is the resource's primary key.
	PrimaryKey []byte

	// Data is the resource payload (nil for DELETE).
	Data []byte

	// ExpectedSeqNum is the client's expected UpdateSeqNum for OCC.
	// Zero means "no expectation" (only meaningful for UPDATE/DELETE).
	ExpectedSeqNum uint64

	// Existing is the current StoredRecord for this PK in the LSM.
	// Nil if the record does not exist.
	Existing *StoredRecord
}

// runAdmission runs all checkers in sequence. Returns the first error, or nil.
func runAdmission(checkers []AdmissionChecker, req *WriteRequest) error {
	for _, c := range checkers {
		if err := c.Check(req); err != nil {
			return err
		}
	}
	return nil
}

// --- Built-in admission checkers ---

// pkUniquenessChecker rejects CREATE when the PK already exists (and is not deleted).
// For non-CREATE operations, this checker is a no-op.
type pkUniquenessChecker struct{}

func (c *pkUniquenessChecker) Check(req *WriteRequest) error {
	if req.Operation != storagev1.StorageOperation_STORAGE_OPERATION_CREATE {
		return nil
	}
	if req.Existing != nil && !req.Existing.IsDeleted {
		return ErrAlreadyExists
	}
	return nil
}

// existenceChecker rejects UPDATE/DELETE when the record does not exist or is deleted.
// For CREATE, this checker is a no-op.
type existenceChecker struct{}

func (c *existenceChecker) Check(req *WriteRequest) error {
	if req.Operation == storagev1.StorageOperation_STORAGE_OPERATION_CREATE {
		return nil
	}
	if req.Existing == nil || req.Existing.IsDeleted {
		return ErrNotFound
	}
	return nil
}

// occChecker validates optimistic concurrency control for UPDATE/DELETE.
// If ExpectedSeqNum is non-zero, it must match the existing record's UpdateSeqNum.
// For CREATE, this checker is a no-op.
type occChecker struct{}

func (c *occChecker) Check(req *WriteRequest) error {
	if req.Operation == storagev1.StorageOperation_STORAGE_OPERATION_CREATE {
		return nil
	}
	// Existence is checked by existenceChecker; if we get here, Existing is non-nil.
	if req.Existing == nil {
		return nil
	}
	if req.ExpectedSeqNum != 0 && req.Existing.UpdateSeqNum != req.ExpectedSeqNum {
		return ErrConflict
	}
	return nil
}

// defaultCheckers returns the standard admission checker pipeline.
func defaultCheckers() []AdmissionChecker {
	return []AdmissionChecker{
		&pkUniquenessChecker{},
		&existenceChecker{},
		&occChecker{},
	}
}
