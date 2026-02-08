package storage

import "errors"

var (
	// ErrClosed is returned when operating on a closed TypeStore or StorageManager.
	ErrClosed = errors.New("storage: closed")

	// ErrNotFound is returned when a resource does not exist or has been deleted.
	ErrNotFound = errors.New("storage: not found")

	// ErrAlreadyExists is returned when creating a resource whose PK already exists.
	ErrAlreadyExists = errors.New("storage: already exists")

	// ErrConflict is returned when an OCC check fails (expectedSeqNum mismatch).
	ErrConflict = errors.New("storage: conflict")

	// ErrNoSchema is returned when the type has no current schema version.
	ErrNoSchema = errors.New("storage: no current schema for type")
)
