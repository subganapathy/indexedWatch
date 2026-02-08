package schema

import (
	"fmt"
	"sync"

	schemav1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/schema/v1"
	"github.com/subganapathy/indexedwatch/pkg/wal"
)

// typeState holds all versions and the current version pointer for a single type.
type typeState struct {
	versions map[string]*Schema // version → Schema
	current  string             // current version name (empty if unset)
}

// Registry is a WAL-backed, in-memory schema registry.
//
// All mutations (Register, Update, SetCurrent) are persisted to a WAL before
// updating in-memory state. On startup, the WAL is replayed via Recover to
// materialize the full registry state.
//
// Reads are lock-free via sync.RWMutex (read-heavy, write-rare workload).
type Registry struct {
	mu    sync.RWMutex
	types map[string]*typeState // typeName → typeState

	w      *wal.WAL
	closed bool
}

// Open creates a new Registry backed by a WAL in the given directory.
// If existing WAL data is found, it is replayed to recover state.
func Open(dir string) (*Registry, error) {
	w, err := wal.Open(dir, wal.SchemaOptions())
	if err != nil {
		return nil, fmt.Errorf("schema: failed to open WAL: %w", err)
	}

	r := &Registry{
		types: make(map[string]*typeState),
		w:     w,
	}

	if err := r.recover(); err != nil {
		w.Close()
		return nil, fmt.Errorf("schema: failed to recover: %w", err)
	}

	return r, nil
}

// Register persists a new schema version to the WAL and adds it to the registry.
// Fails if the version already exists for this type.
// Does NOT set the version as current — use SetCurrent for that.
func (r *Registry) Register(s *Schema) error {
	if err := s.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRegistryClosed
	}

	ts := r.types[s.Type]
	if ts != nil {
		if _, exists := ts.versions[s.Version]; exists {
			return fmt.Errorf("%w: %s/%s", ErrVersionExists, s.Type, s.Version)
		}
	}

	data, err := marshalRegister(s)
	if err != nil {
		return fmt.Errorf("schema: failed to marshal register entry: %w", err)
	}

	if _, err := r.w.Append(data); err != nil {
		return fmt.Errorf("schema: failed to append to WAL: %w", err)
	}
	if err := r.w.Sync(); err != nil {
		return fmt.Errorf("schema: failed to sync WAL: %w", err)
	}

	r.applyRegister(s)
	return nil
}

// Update persists an updated schema version to the WAL and applies it.
// The type and version must already exist. Evolution rules are enforced:
//   - Primary key cannot change
//   - Existing field types cannot change
//   - New required fields must have a default value
func (r *Registry) Update(s *Schema) error {
	if err := s.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRegistryClosed
	}

	ts := r.types[s.Type]
	if ts == nil {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, s.Type)
	}

	prev, exists := ts.versions[s.Version]
	if !exists {
		return fmt.Errorf("%w: %s/%s", ErrVersionNotFound, s.Type, s.Version)
	}

	if err := ValidateEvolution(prev, s); err != nil {
		return err
	}

	data, err := marshalUpdate(s)
	if err != nil {
		return fmt.Errorf("schema: failed to marshal update entry: %w", err)
	}

	if _, err := r.w.Append(data); err != nil {
		return fmt.Errorf("schema: failed to append to WAL: %w", err)
	}
	if err := r.w.Sync(); err != nil {
		return fmt.Errorf("schema: failed to sync WAL: %w", err)
	}

	r.applyUpdate(s)
	return nil
}

// SetCurrent sets the active version for a type. The type and version must exist.
// All new resource writes are validated against the current schema version.
func (r *Registry) SetCurrent(typeName, version string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRegistryClosed
	}

	ts := r.types[typeName]
	if ts == nil {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
	}
	if _, exists := ts.versions[version]; !exists {
		return fmt.Errorf("%w: %s/%s", ErrVersionNotFound, typeName, version)
	}

	data, err := marshalSetCurrent(typeName, version)
	if err != nil {
		return fmt.Errorf("schema: failed to marshal set-current entry: %w", err)
	}

	if _, err := r.w.Append(data); err != nil {
		return fmt.Errorf("schema: failed to append to WAL: %w", err)
	}
	if err := r.w.Sync(); err != nil {
		return fmt.Errorf("schema: failed to sync WAL: %w", err)
	}

	ts.current = version
	return nil
}

// Get returns a specific schema version. Returns ErrTypeNotFound or
// ErrVersionNotFound if the type or version does not exist.
func (r *Registry) Get(typeName, version string) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrRegistryClosed
	}

	ts := r.types[typeName]
	if ts == nil {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
	}

	s, exists := ts.versions[version]
	if !exists {
		return nil, fmt.Errorf("%w: %s/%s", ErrVersionNotFound, typeName, version)
	}

	return s, nil
}

// GetCurrent returns the current (active) schema version for a type.
func (r *Registry) GetCurrent(typeName string) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrRegistryClosed
	}

	ts := r.types[typeName]
	if ts == nil {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
	}

	if ts.current == "" {
		return nil, fmt.Errorf("%w: %s", ErrNoCurrentVersion, typeName)
	}

	return ts.versions[ts.current], nil
}

// ListTypes returns the names of all registered schema types.
func (r *Registry) ListTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.types))
	for t := range r.types {
		types = append(types, t)
	}
	return types
}

// ListVersions returns all version names for a type, or ErrTypeNotFound.
func (r *Registry) ListVersions(typeName string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ts := r.types[typeName]
	if ts == nil {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
	}

	versions := make([]string, 0, len(ts.versions))
	for v := range ts.versions {
		versions = append(versions, v)
	}
	return versions, nil
}

// Close closes the underlying WAL and marks the registry as closed.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	return r.w.Close()
}

// recover replays all WAL entries to materialize in-memory state.
// Called during Open before the registry is returned to callers.
func (r *Registry) recover() error {
	entries, err := r.w.ReadAll()
	if err != nil {
		return err
	}

	for i, data := range entries {
		entry, err := unmarshalEntry(data)
		if err != nil {
			return fmt.Errorf("schema: failed to unmarshal entry %d: %w", i, err)
		}

		switch entry.GetOperation() {
		case schemav1.SchemaOperation_SCHEMA_OPERATION_REGISTER:
			s := FromProto(entry.GetSchema())
			if s == nil {
				return fmt.Errorf("schema: nil schema in REGISTER entry %d", i)
			}
			r.applyRegister(s)

		case schemav1.SchemaOperation_SCHEMA_OPERATION_UPDATE:
			s := FromProto(entry.GetSchema())
			if s == nil {
				return fmt.Errorf("schema: nil schema in UPDATE entry %d", i)
			}
			r.applyUpdate(s)

		case schemav1.SchemaOperation_SCHEMA_OPERATION_SET_CURRENT:
			typeName := entry.GetType()
			version := entry.GetVersion()
			ts := r.types[typeName]
			if ts != nil {
				ts.current = version
			}

		default:
			return fmt.Errorf("schema: unknown operation %d in entry %d", entry.GetOperation(), i)
		}
	}

	return nil
}

// applyRegister adds a schema to the in-memory state. Caller must hold w.mu.
func (r *Registry) applyRegister(s *Schema) {
	ts := r.types[s.Type]
	if ts == nil {
		ts = &typeState{
			versions: make(map[string]*Schema),
		}
		r.types[s.Type] = ts
	}
	ts.versions[s.Version] = s
}

// applyUpdate replaces a schema version in-memory. Caller must hold w.mu.
func (r *Registry) applyUpdate(s *Schema) {
	ts := r.types[s.Type]
	if ts == nil {
		ts = &typeState{
			versions: make(map[string]*Schema),
		}
		r.types[s.Type] = ts
	}
	ts.versions[s.Version] = s
}
