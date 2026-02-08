// Package schema provides a WAL-backed schema registry for resource types.
//
// The registry materializes schema state from WAL replay on startup,
// providing the (type, version) → Schema lookup every storage component needs.
// Schema changes are persisted to a dedicated WAL (using SchemaOptions) for
// durability, and replayed on recovery.
package schema

import "errors"

var (
	// ErrTypeNotFound is returned when the requested schema type does not exist.
	ErrTypeNotFound = errors.New("schema: type not found")

	// ErrVersionNotFound is returned when the requested version does not exist for the type.
	ErrVersionNotFound = errors.New("schema: version not found")

	// ErrVersionExists is returned when registering a version that already exists.
	ErrVersionExists = errors.New("schema: version already exists")

	// ErrNoCurrentVersion is returned when no current version has been set for a type.
	ErrNoCurrentVersion = errors.New("schema: no current version set")

	// ErrPrimaryKeyChanged is returned when an update attempts to change the primary key.
	ErrPrimaryKeyChanged = errors.New("schema: primary key cannot be changed")

	// ErrFieldTypeChanged is returned when an update attempts to change a field's type.
	ErrFieldTypeChanged = errors.New("schema: field type cannot be changed")

	// ErrRequiredFieldNoDefault is returned when a new required field lacks a default value.
	ErrRequiredFieldNoDefault = errors.New("schema: new required field must have a default value")

	// ErrInvalidSchema is returned when the schema definition is malformed.
	ErrInvalidSchema = errors.New("schema: invalid schema definition")

	// ErrRegistryClosed is returned when operating on a closed registry.
	ErrRegistryClosed = errors.New("schema: registry closed")
)
