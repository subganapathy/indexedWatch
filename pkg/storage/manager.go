package storage

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/subganapathy/indexedwatch/pkg/schema"
)

// StorageManager manages per-type TypeStores, creating them on demand
// when a resource type is first accessed.
//
// Each resource type gets its own directory containing a WAL and LSM:
//
//	baseDir/
//	  types/
//	    events/
//	      wal/
//	      lsm/
//	    users/
//	      wal/
//	      lsm/
type StorageManager struct {
	baseDir  string
	registry *schema.Registry
	opts     TypeStoreOptions

	mu    sync.Mutex
	types map[string]*TypeStore

	closed bool
}

// OpenStorageManager creates a new StorageManager.
// It does not open any TypeStores immediately — they are opened on first access.
func OpenStorageManager(baseDir string, registry *schema.Registry, opts TypeStoreOptions) *StorageManager {
	return &StorageManager{
		baseDir:  baseDir,
		registry: registry,
		opts:     opts,
		types:    make(map[string]*TypeStore),
	}
}

// GetTypeStore returns the TypeStore for the given resource type,
// opening it if it doesn't exist yet.
func (sm *StorageManager) GetTypeStore(typeName string) (*TypeStore, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil, ErrClosed
	}

	if ts, ok := sm.types[typeName]; ok {
		return ts, nil
	}

	dir := filepath.Join(sm.baseDir, "types", typeName)
	ts, err := OpenTypeStore(dir, typeName, sm.registry, sm.opts)
	if err != nil {
		return nil, fmt.Errorf("storage: open type store %s: %w", typeName, err)
	}

	sm.types[typeName] = ts
	return ts, nil
}

// Close shuts down all open TypeStores.
func (sm *StorageManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return nil
	}
	sm.closed = true

	var firstErr error
	for name, ts := range sm.types {
		if err := ts.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("storage: close %s: %w", name, err)
		}
	}
	sm.types = nil

	return firstErr
}
