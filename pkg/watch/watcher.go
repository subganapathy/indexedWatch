package watch

import (
	"context"
	"fmt"
	"sync"
)

// SnapshotFunc is called by the watcher to take a point-in-time snapshot.
// It returns the snapshot seqNum and a list of all non-deleted resources.
// The implementation is provided by TypeStore.
type SnapshotFunc func() (seqNum uint64, resources []SnapshotResource, err error)

// SnapshotResource is a single resource included in a snapshot.
type SnapshotResource struct {
	PrimaryKey []byte
	Data       []byte
}

// WatchResponse is a single message sent to a watch client.
// Either Snapshot or Events is populated, never both.
type WatchResponse struct {
	// Snapshot is populated during the snapshot phase (initial state).
	// Nil during the delta phase.
	Snapshot *SnapshotResponse

	// Events is populated during the delta phase (incremental updates).
	// Nil during the snapshot phase.
	Events []Event
}

// SnapshotResponse contains the initial state of all resources at a point-in-time.
type SnapshotResponse struct {
	// SeqNum is the storage seqNum at the time the snapshot was taken.
	// Delta events will start from SeqNum+1.
	SeqNum uint64

	// Resources contains all non-deleted resources at snapshot time.
	Resources []SnapshotResource
}

// Watcher manages a single client's watch lifecycle:
//
//  1. Snapshot phase: If fromSeqNum is 0, take a snapshot and send all current state.
//  2. Delta phase: Stream events from the ring buffer starting at the appropriate seqNum.
//     Subscribe to notifications for new events.
//  3. On ErrExpired: The watcher returns the error; the client must re-watch from 0.
//
// The watcher sends responses to the provided channel and blocks until the context
// is cancelled or an unrecoverable error occurs.
type Watcher struct {
	rb         *RingBuffer
	snapFn     SnapshotFunc
	fromSeqNum uint64

	mu     sync.Mutex
	closed bool
}

// NewWatcher creates a watcher that will start from the given seqNum.
// If fromSeqNum is 0, the watcher performs a snapshot phase first.
// snapFn is called during the snapshot phase to capture initial state.
func NewWatcher(rb *RingBuffer, snapFn SnapshotFunc, fromSeqNum uint64) *Watcher {
	return &Watcher{
		rb:         rb,
		snapFn:     snapFn,
		fromSeqNum: fromSeqNum,
	}
}

// Watch runs the watch loop, sending responses to the provided channel.
// It blocks until ctx is cancelled, the ring buffer is closed, or an
// unrecoverable error (like ErrExpired) occurs.
//
// The channel is NOT closed by Watch — the caller owns it.
func (w *Watcher) Watch(ctx context.Context, ch chan<- WatchResponse) error {
	nextSeqNum := w.fromSeqNum

	// Snapshot phase: if starting from 0, capture initial state.
	if nextSeqNum == 0 {
		if w.snapFn == nil {
			return fmt.Errorf("watch: snapshot function required when fromSeqNum=0")
		}

		snapSeqNum, resources, err := w.snapFn()
		if err != nil {
			return fmt.Errorf("watch: snapshot: %w", err)
		}

		resp := WatchResponse{
			Snapshot: &SnapshotResponse{
				SeqNum:    snapSeqNum,
				Resources: resources,
			},
		}

		select {
		case ch <- resp:
		case <-ctx.Done():
			return ctx.Err()
		}

		nextSeqNum = snapSeqNum + 1
	}

	// Delta phase: read existing buffered events, then subscribe for new ones.
	subID, notifyCh := w.rb.Subscribe()
	defer w.rb.Unsubscribe(subID)

	for {
		// Read any events from the ring buffer >= nextSeqNum.
		events, err := w.rb.ReadFrom(nextSeqNum)
		if err != nil {
			return err // ErrExpired or ErrClosed
		}

		if len(events) > 0 {
			resp := WatchResponse{Events: events}
			select {
			case ch <- resp:
			case <-ctx.Done():
				return ctx.Err()
			}
			nextSeqNum = events[len(events)-1].SeqNum + 1
		}

		// Wait for new events or cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-notifyCh:
			if !ok {
				// Channel closed (ring buffer closed or unsubscribed).
				return ErrClosed
			}
			// New events available — loop back to ReadFrom.
		}
	}
}

// Close marks the watcher as closed.
func (w *Watcher) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
}
