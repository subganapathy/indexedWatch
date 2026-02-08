package watch

import (
	"context"
	"sync"
)

// DefaultRingBufferCapacity is the default number of events each ring buffer holds.
const DefaultRingBufferCapacity = 4096

// ManagerOptions configures the watch manager.
type ManagerOptions struct {
	// RingBufferCapacity is the number of events each per-type ring buffer holds.
	// If zero, DefaultRingBufferCapacity is used.
	RingBufferCapacity int
}

// Manager coordinates watch streams for all resource types.
// It owns one RingBuffer per type and manages watcher lifecycle.
//
// Integration with TypeStore:
//   - TypeStore calls Manager.Notify(typeName, event) after each write.
//   - Watch clients call Manager.Watch(ctx, typeName, fromSeqNum, snapFn)
//     to receive a stream of responses.
type Manager struct {
	mu      sync.RWMutex
	buffers map[string]*RingBuffer
	opts    ManagerOptions
	closed  bool
}

// NewManager creates a watch manager with the given options.
func NewManager(opts ManagerOptions) *Manager {
	if opts.RingBufferCapacity <= 0 {
		opts.RingBufferCapacity = DefaultRingBufferCapacity
	}
	return &Manager{
		buffers: make(map[string]*RingBuffer),
		opts:    opts,
	}
}

// Notify appends an event to the ring buffer for the given type.
// Creates the ring buffer lazily if it doesn't exist yet.
// This is called by TypeStore on each Create/Update/Delete.
func (m *Manager) Notify(typeName string, event Event) {
	rb := m.getOrCreateBuffer(typeName)
	rb.Append(event)
}

// Watch starts a watch stream for the given type, sending responses to the
// returned channel. It blocks until ctx is cancelled or an error occurs.
//
// If fromSeqNum is 0, the watcher takes a snapshot first (using snapFn),
// then transitions to streaming deltas from the ring buffer.
//
// If fromSeqNum > 0, the watcher streams deltas directly from the ring buffer.
// Returns ErrExpired if fromSeqNum is older than the oldest buffered event.
//
// The returned channel is closed when Watch returns.
func (m *Manager) Watch(ctx context.Context, typeName string, fromSeqNum uint64, snapFn SnapshotFunc) (<-chan WatchResponse, <-chan error) {
	respCh := make(chan WatchResponse, 16)
	errCh := make(chan error, 1)

	rb := m.getOrCreateBuffer(typeName)
	w := NewWatcher(rb, snapFn, fromSeqNum)

	go func() {
		defer close(respCh)
		err := w.Watch(ctx, respCh)
		if err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	return respCh, errCh
}

// OldestSeqNum returns the oldest seqNum in the ring buffer for the given type.
// Returns 0 if no ring buffer exists for the type or if it's empty.
// Used by WAL pruning to ensure we don't prune events still needed by watchers.
func (m *Manager) OldestSeqNum(typeName string) uint64 {
	m.mu.RLock()
	rb, ok := m.buffers[typeName]
	m.mu.RUnlock()

	if !ok {
		return 0
	}
	return rb.OldestSeqNum()
}

// Buffer returns the ring buffer for the given type, or nil if it doesn't exist.
// Mainly used for testing and diagnostics.
func (m *Manager) Buffer(typeName string) *RingBuffer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.buffers[typeName]
}

// Close shuts down the manager and all ring buffers.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	for _, rb := range m.buffers {
		rb.Close()
	}
}

// getOrCreateBuffer returns the ring buffer for the type, creating it if needed.
func (m *Manager) getOrCreateBuffer(typeName string) *RingBuffer {
	m.mu.RLock()
	rb, ok := m.buffers[typeName]
	m.mu.RUnlock()

	if ok {
		return rb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	if rb, ok = m.buffers[typeName]; ok {
		return rb
	}

	rb = NewRingBuffer(m.opts.RingBufferCapacity)
	m.buffers[typeName] = rb
	return rb
}
