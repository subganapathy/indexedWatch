package watch

import (
	"context"
	"testing"
	"time"
)

func makeSnapFn(seqNum uint64, resources []SnapshotResource) SnapshotFunc {
	return func() (uint64, []SnapshotResource, error) {
		return seqNum, resources, nil
	}
}

func TestWatcherSnapshotPhase(t *testing.T) {
	rb := NewRingBuffer(64)

	resources := []SnapshotResource{
		{PrimaryKey: []byte("key-1"), Data: []byte("data-1")},
		{PrimaryKey: []byte("key-2"), Data: []byte("data-2")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	respCh := make(chan WatchResponse, 16)

	// Pre-populate ring buffer with events after snapshot point.
	rb.Append(Event{SeqNum: 6, Operation: OpCreate, PrimaryKey: []byte("key-3")})

	w := NewWatcher(rb, makeSnapFn(5, resources), 0)
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Watch(ctx, respCh)
	}()

	// First response should be the snapshot.
	select {
	case resp := <-respCh:
		if resp.Snapshot == nil {
			t.Fatal("expected snapshot response")
		}
		if resp.Snapshot.SeqNum != 5 {
			t.Fatalf("snapshot seqNum: expected 5, got %d", resp.Snapshot.SeqNum)
		}
		if len(resp.Snapshot.Resources) != 2 {
			t.Fatalf("snapshot resources: expected 2, got %d", len(resp.Snapshot.Resources))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for snapshot response")
	}

	// Second response should be the delta events (seqNum 6).
	select {
	case resp := <-respCh:
		if resp.Events == nil {
			t.Fatal("expected events response")
		}
		if len(resp.Events) != 1 || resp.Events[0].SeqNum != 6 {
			t.Fatalf("expected event seqNum 6, got %v", resp.Events)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delta response")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for watcher to exit")
	}
}

func TestWatcherDeltaOnly(t *testing.T) {
	rb := NewRingBuffer(64)

	// Pre-populate with events.
	rb.Append(Event{SeqNum: 10, Operation: OpCreate, PrimaryKey: []byte("a")})
	rb.Append(Event{SeqNum: 11, Operation: OpUpdate, PrimaryKey: []byte("b")})
	rb.Append(Event{SeqNum: 12, Operation: OpDelete, PrimaryKey: []byte("c")})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	respCh := make(chan WatchResponse, 16)

	// Start from seqNum 11 (no snapshot phase).
	w := NewWatcher(rb, nil, 11)
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Watch(ctx, respCh)
	}()

	// Should get events 11 and 12.
	select {
	case resp := <-respCh:
		if resp.Snapshot != nil {
			t.Fatal("should not get snapshot when fromSeqNum > 0")
		}
		if len(resp.Events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(resp.Events))
		}
		if resp.Events[0].SeqNum != 11 || resp.Events[1].SeqNum != 12 {
			t.Fatalf("wrong events: %v", resp.Events)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delta response")
	}

	cancel()
}

func TestWatcherExpired(t *testing.T) {
	rb := NewRingBuffer(4)

	// Fill buffer so oldest is seqNum 4.
	for i := uint64(1); i <= 7; i++ {
		rb.Append(Event{SeqNum: i})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	respCh := make(chan WatchResponse, 16)

	// Try to resume from seqNum 2 — too old.
	w := NewWatcher(rb, nil, 2)
	err := w.Watch(ctx, respCh)
	if err != ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
}

func TestWatcherLiveUpdates(t *testing.T) {
	rb := NewRingBuffer(64)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	respCh := make(chan WatchResponse, 16)

	// Start watching with snapshot.
	w := NewWatcher(rb, makeSnapFn(0, nil), 0)
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Watch(ctx, respCh)
	}()

	// Consume snapshot (empty).
	select {
	case resp := <-respCh:
		if resp.Snapshot == nil {
			t.Fatal("expected snapshot response")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for snapshot")
	}

	// Now append events — watcher should pick them up.
	rb.Append(Event{SeqNum: 1, Operation: OpCreate, PrimaryKey: []byte("new-key")})

	select {
	case resp := <-respCh:
		if len(resp.Events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(resp.Events))
		}
		if resp.Events[0].SeqNum != 1 {
			t.Fatalf("expected seqNum 1, got %d", resp.Events[0].SeqNum)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for live event")
	}

	// Append more.
	rb.Append(Event{SeqNum: 2, Operation: OpUpdate, PrimaryKey: []byte("new-key")})
	rb.Append(Event{SeqNum: 3, Operation: OpDelete, PrimaryKey: []byte("old-key")})

	select {
	case resp := <-respCh:
		if len(resp.Events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(resp.Events))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for batch of live events")
	}

	cancel()
}

func TestWatcherCancelDuringSnapshot(t *testing.T) {
	rb := NewRingBuffer(64)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately.
	cancel()

	respCh := make(chan WatchResponse) // unbuffered — will block on send

	w := NewWatcher(rb, makeSnapFn(5, []SnapshotResource{{PrimaryKey: []byte("k")}}), 0)
	err := w.Watch(ctx, respCh)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWatcherNoSnapFnWithSeqZero(t *testing.T) {
	rb := NewRingBuffer(64)

	ctx := context.Background()
	respCh := make(chan WatchResponse, 16)

	// fromSeqNum=0 but no snapFn — should error.
	w := NewWatcher(rb, nil, 0)
	err := w.Watch(ctx, respCh)
	if err == nil {
		t.Fatal("expected error when snapFn is nil and fromSeqNum=0")
	}
}
