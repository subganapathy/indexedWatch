package wal

import "sync"

// syncGroup implements leader-follower group commit. Multiple concurrent
// Sync() callers share a single fdatasync by waiting for the current
// leader to complete the sync on their behalf.
//
// The first goroutine to arrive becomes the leader and performs the
// actual sync. Subsequent goroutines that arrive while a sync is in
// progress simply wait for the leader's result.
type syncGroup struct {
	mu       sync.Mutex
	pending  []chan error // Waiting followers
	syncing  bool        // Whether a sync is currently in progress
	syncFunc func() error
}

// newSyncGroup creates a new syncGroup that calls syncFunc to perform the
// actual sync (typically: flush encoder + fdatasync).
func newSyncGroup(syncFunc func() error) *syncGroup {
	return &syncGroup{
		syncFunc: syncFunc,
	}
}

// sync performs a group-committed sync. If a sync is already in progress,
// the caller waits for it to complete and receives the same error result.
// If no sync is in progress, the caller becomes the leader.
func (sg *syncGroup) sync() error {
	sg.mu.Lock()

	if sg.syncing {
		// A sync is already in progress — become a follower.
		ch := make(chan error, 1)
		sg.pending = append(sg.pending, ch)
		sg.mu.Unlock()
		return <-ch
	}

	// Become the leader.
	sg.syncing = true
	sg.mu.Unlock()

	// Perform the actual sync.
	err := sg.syncFunc()

	// Wake all followers with the result.
	sg.mu.Lock()
	waiters := sg.pending
	sg.pending = nil
	sg.syncing = false
	sg.mu.Unlock()

	for _, ch := range waiters {
		ch <- err
	}

	return err
}
