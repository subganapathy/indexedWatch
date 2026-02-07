package wal

import "sync"

// syncGroup implements leader-follower group commit. Multiple concurrent
// Sync() callers share a single fdatasync by waiting for the current
// leader to complete the sync on their behalf.
//
// Correctness invariant: syncFunc must hold w.mu during flush+fsync.
// This ensures that any goroutine whose Append() completed before the
// flush started will have its data covered by the sync. Goroutines that
// sneak into pending between syncFunc returning and the leader collecting
// waiters are handled by looping: the leader re-syncs if new pending
// appeared, guaranteeing their data is also flushed.
type syncGroup struct {
	mu       sync.Mutex
	pending  []chan error // Waiting followers
	syncing  bool        // Whether a sync is currently in progress
	syncFunc func() error
}

// newSyncGroup creates a new syncGroup that calls syncFunc to perform the
// actual sync. syncFunc MUST acquire w.mu internally (flush + fsync under lock).
func newSyncGroup(syncFunc func() error) *syncGroup {
	return &syncGroup{
		syncFunc: syncFunc,
	}
}

// sync performs a group-committed sync. If a sync is already in progress,
// the caller waits for a leader to pick it up. If no sync is in progress,
// the caller becomes the leader.
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

	var lastErr error
	for {
		// Take the current batch of waiters.
		batch := sg.pending
		sg.pending = nil
		sg.mu.Unlock()

		// Perform the actual sync (holds w.mu during flush+fsync).
		lastErr = sg.syncFunc()

		// Notify this batch.
		for _, ch := range batch {
			ch <- lastErr
		}

		// Check if more followers arrived during the sync.
		// Their Append() completed after our flush, so we must
		// re-sync to cover their data.
		sg.mu.Lock()
		if len(sg.pending) == 0 {
			break
		}
		// Loop: re-sync to cover latecomers.
	}

	sg.syncing = false
	sg.mu.Unlock()

	return lastErr
}
