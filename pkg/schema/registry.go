package schema

import (
	"fmt"
	"sync"

	schemav1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/schema/v1"
	"github.com/subganapathy/indexedwatch/pkg/wal"
)

// Registry defines the interface for a schema registry.
// Downstream packages (e.g., storage) depend on this interface for testability.
type Registry interface {
	Register(s *Schema) error
	AddIndex(typeName, indexPath string) error
	RemoveIndex(typeName, indexPath string) error
	Get(typeName string) (*Schema, error)
	ListTypes() []string
	Close() error
}

// walRegistry is a WAL-backed, in-memory schema registry.
//
// All mutations (Register, AddIndex, RemoveIndex) are persisted to a WAL before
// updating in-memory state. On startup, the WAL is replayed via recover to
// materialize the full registry state.
//
// Reads are lock-free via sync.RWMutex (read-heavy, write-rare workload).
type walRegistry struct {
	mu    sync.RWMutex
	types map[string]*Schema // typeName → Schema

	w      *wal.WAL
	closed bool
}

// Open creates a new Registry backed by a WAL in the given directory.
// If existing WAL data is found, it is replayed to recover state.
func Open(dir string) (Registry, error) {
	w, err := wal.Open(dir, wal.SchemaOptions())
	if err != nil {
		return nil, fmt.Errorf("schema: failed to open WAL: %w", err)
	}

	r := &walRegistry{
		types: make(map[string]*Schema),
		w:     w,
	}

	if err := r.recover(); err != nil {
		w.Close()
		return nil, fmt.Errorf("schema: failed to recover: %w", err)
	}

	return r, nil
}

// Register persists a new schema definition to the WAL and adds it to the registry.
// Fails if the type already exists — Register is a one-time operation per type.
// Use AddIndex/RemoveIndex to evolve the index set.
func (r *walRegistry) Register(s *Schema) error {
	if err := s.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRegistryClosed
	}

	if _, exists := r.types[s.Type]; exists {
		return fmt.Errorf("%w: %s", ErrTypeExists, s.Type)
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

// AddIndex adds a secondary index to an existing type.
// Persists an ADD_INDEX entry to the WAL, then updates in-memory state.
// Returns ErrIndexExists if the index already exists on this type.
func (r *walRegistry) AddIndex(typeName, indexPath string) error {
	if indexPath == "" {
		return fmt.Errorf("%w: index path cannot be empty", ErrInvalidSchema)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRegistryClosed
	}

	s := r.types[typeName]
	if s == nil {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
	}

	if s.HasIndex(indexPath) {
		return fmt.Errorf("%w: %s on %s", ErrIndexExists, indexPath, typeName)
	}

	if indexPath == s.PrimaryKey {
		return fmt.Errorf("%w: cannot add primary key as secondary index", ErrInvalidSchema)
	}

	data, err := marshalAddIndex(typeName, indexPath)
	if err != nil {
		return fmt.Errorf("schema: failed to marshal add-index entry: %w", err)
	}

	if _, err := r.w.Append(data); err != nil {
		return fmt.Errorf("schema: failed to append to WAL: %w", err)
	}
	if err := r.w.Sync(); err != nil {
		return fmt.Errorf("schema: failed to sync WAL: %w", err)
	}

	r.applyAddIndex(typeName, indexPath)
	return nil
}

// RemoveIndex removes a secondary index from an existing type.
// Persists a REMOVE_INDEX entry to the WAL, then updates in-memory state.
// Returns ErrIndexNotFound if the index doesn't exist on this type.
func (r *walRegistry) RemoveIndex(typeName, indexPath string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRegistryClosed
	}

	s := r.types[typeName]
	if s == nil {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
	}

	if !s.HasIndex(indexPath) {
		return fmt.Errorf("%w: %s on %s", ErrIndexNotFound, indexPath, typeName)
	}

	data, err := marshalRemoveIndex(typeName, indexPath)
	if err != nil {
		return fmt.Errorf("schema: failed to marshal remove-index entry: %w", err)
	}

	if _, err := r.w.Append(data); err != nil {
		return fmt.Errorf("schema: failed to append to WAL: %w", err)
	}
	if err := r.w.Sync(); err != nil {
		return fmt.Errorf("schema: failed to sync WAL: %w", err)
	}

	r.applyRemoveIndex(typeName, indexPath)
	return nil
}

// Get returns the current schema definition for a type.
func (r *walRegistry) Get(typeName string) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrRegistryClosed
	}

	s := r.types[typeName]
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
	}

	return s, nil
}

// ListTypes returns the names of all registered schema types.
func (r *walRegistry) ListTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.types))
	for t := range r.types {
		types = append(types, t)
	}
	return types
}

// Close closes the underlying WAL and marks the registry as closed.
func (r *walRegistry) Close() error {
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
func (r *walRegistry) recover() error {
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

		case schemav1.SchemaOperation_SCHEMA_OPERATION_ADD_INDEX:
			r.applyAddIndex(entry.GetType(), entry.GetIndexPath())

		case schemav1.SchemaOperation_SCHEMA_OPERATION_REMOVE_INDEX:
			r.applyRemoveIndex(entry.GetType(), entry.GetIndexPath())

		default:
			return fmt.Errorf("schema: unknown operation %d in entry %d", entry.GetOperation(), i)
		}
	}

	return nil
}

// applyRegister adds a schema to the in-memory state.
// Caller must hold r.mu (either via Lock or during single-threaded recover).
func (r *walRegistry) applyRegister(s *Schema) {
	indexes := make([]string, len(s.SecondaryIndexes))
	copy(indexes, s.SecondaryIndexes)

	r.types[s.Type] = &Schema{
		Type:             s.Type,
		PrimaryKey:       s.PrimaryKey,
		SecondaryIndexes: indexes,
	}
}

// applyAddIndex appends an index to a type's secondary index list.
// Caller must hold r.mu (either via Lock or during single-threaded recover).
func (r *walRegistry) applyAddIndex(typeName, indexPath string) {
	s := r.types[typeName]
	if s == nil {
		return // type not found during replay — skip
	}
	if !s.HasIndex(indexPath) {
		s.SecondaryIndexes = append(s.SecondaryIndexes, indexPath)
	}
}

// applyRemoveIndex removes an index from a type's secondary index list.
// Caller must hold r.mu (either via Lock or during single-threaded recover).
func (r *walRegistry) applyRemoveIndex(typeName, indexPath string) {
	s := r.types[typeName]
	if s == nil {
		return // type not found during replay — skip
	}
	filtered := s.SecondaryIndexes[:0]
	for _, idx := range s.SecondaryIndexes {
		if idx != indexPath {
			filtered = append(filtered, idx)
		}
	}
	s.SecondaryIndexes = filtered
}
