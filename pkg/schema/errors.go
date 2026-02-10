// Package schema provides a WAL-backed schema registry for resource types.
//
// The registry materializes schema state from WAL replay on startup,
// providing the type → Schema lookup every storage component needs.
// Schema changes are persisted to a dedicated WAL for durability,
// and replayed on recovery.
package schema

import "errors"

var (
	// ErrTypeNotFound is returned when the requested schema type does not exist.
	ErrTypeNotFound = errors.New("schema: type not found")

	// ErrTypeExists is returned when registering a type that already exists.
	ErrTypeExists = errors.New("schema: type already exists")

	// ErrIndexExists is returned when adding an index that already exists.
	ErrIndexExists = errors.New("schema: index already exists")

	// ErrIndexNotFound is returned when removing an index that doesn't exist.
	ErrIndexNotFound = errors.New("schema: index not found")

	// ErrInvalidSchema is returned when the schema definition is malformed.
	ErrInvalidSchema = errors.New("schema: invalid schema definition")

	// ErrRegistryClosed is returned when operating on a closed registry.
	ErrRegistryClosed = errors.New("schema: registry closed")
)
