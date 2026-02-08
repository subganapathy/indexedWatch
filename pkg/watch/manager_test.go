package watch

import (
	"context"
	"testing"
	"time"
)

func TestManagerNotifyCreatesBuffer(t *testing.T) {
	m := NewManager(ManagerOptions{RingBufferCapacity: 64})
	defer m.Close()

	if m.Buffer("events") != nil {
		t.Fatal("buffer should not exist before Notify")
	}

	m.Notify("events", Event{SeqNum: 1, Operation: OpCreate})

	rb := m.Buffer("events")
	if rb == nil {
		t.Fatal("buffer should exist after Notify")
	}
	if rb.Len() != 1 {
		t.Fatalf("expected 1 event, got %d", rb.Len())
	}
}

func TestManagerOldestSeqNum(t *testing.T) {
	m := NewManager(ManagerOptions{RingBufferCapacity: 4})
	defer m.Close()

	// No buffer for this type yet.
	if m.OldestSeqNum("events") != 0 {
		t.Fatal("expected 0 for nonexistent type")
	}

	for i := uint64(1); i <= 6; i++ {
		m.Notify("events", Event{SeqNum: i})
	}

	// Buffer capacity is 4, so oldest should be 3 (events 3,4,5,6 remain).
	oldest := m.OldestSeqNum("events")
	if oldest != 3 {
		t.Fatalf("oldest: expected 3, got %d", oldest)
	}
}

func TestManagerWatch(t *testing.T) {
	m := NewManager(ManagerOptions{RingBufferCapacity: 64})
	defer m.Close()

	snapFn := makeSnapFn(0, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	respCh, errCh := m.Watch(ctx, "events", 0, snapFn)

	// Should get snapshot first.
	select {
	case resp, ok := <-respCh:
		if !ok {
			t.Fatal("respCh closed unexpectedly")
		}
		if resp.Snapshot == nil {
			t.Fatal("expected snapshot response")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for snapshot")
	}

	// Notify some events.
	m.Notify("events", Event{SeqNum: 1, Operation: OpCreate, PrimaryKey: []byte("k1")})
	m.Notify("events", Event{SeqNum: 2, Operation: OpUpdate, PrimaryKey: []byte("k2")})

	// Should get deltas.
	select {
	case resp, ok := <-respCh:
		if !ok {
			t.Fatal("respCh closed unexpectedly")
		}
		if resp.Events == nil {
			t.Fatal("expected events response")
		}
		if len(resp.Events) < 1 {
			t.Fatal("expected at least 1 event")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for events")
	}

	cancel()

	// Error channel should eventually yield.
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for errCh")
	}
}

func TestManagerWatchResume(t *testing.T) {
	m := NewManager(ManagerOptions{RingBufferCapacity: 64})
	defer m.Close()

	// Pre-populate events.
	for i := uint64(1); i <= 5; i++ {
		m.Notify("events", Event{SeqNum: i, Operation: OpCreate})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Resume from seqNum 3.
	respCh, _ := m.Watch(ctx, "events", 3, nil)

	select {
	case resp := <-respCh:
		if resp.Snapshot != nil {
			t.Fatal("should not get snapshot on resume")
		}
		if len(resp.Events) != 3 {
			t.Fatalf("expected 3 events (3,4,5), got %d", len(resp.Events))
		}
		if resp.Events[0].SeqNum != 3 || resp.Events[2].SeqNum != 5 {
			t.Fatalf("wrong events: first=%d, last=%d",
				resp.Events[0].SeqNum, resp.Events[len(resp.Events)-1].SeqNum)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for resumed events")
	}

	cancel()
}

func TestManagerWatchExpired(t *testing.T) {
	m := NewManager(ManagerOptions{RingBufferCapacity: 4})
	defer m.Close()

	// Fill buffer past capacity.
	for i := uint64(1); i <= 10; i++ {
		m.Notify("events", Event{SeqNum: i})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Try to resume from expired seqNum.
	_, errCh := m.Watch(ctx, "events", 1, nil)

	select {
	case err := <-errCh:
		if err != ErrExpired {
			t.Fatalf("expected ErrExpired, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for expired error")
	}
}

func TestManagerMultipleTypes(t *testing.T) {
	m := NewManager(ManagerOptions{RingBufferCapacity: 64})
	defer m.Close()

	m.Notify("events", Event{SeqNum: 1, Operation: OpCreate})
	m.Notify("users", Event{SeqNum: 1, Operation: OpCreate})
	m.Notify("events", Event{SeqNum: 2, Operation: OpUpdate})

	if m.Buffer("events").Len() != 2 {
		t.Fatalf("events: expected 2 events")
	}
	if m.Buffer("users").Len() != 1 {
		t.Fatalf("users: expected 1 event")
	}
}

func TestManagerClose(t *testing.T) {
	m := NewManager(ManagerOptions{RingBufferCapacity: 64})

	m.Notify("events", Event{SeqNum: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	respCh, errCh := m.Watch(ctx, "events", 0, makeSnapFn(0, nil))

	// Consume snapshot.
	select {
	case <-respCh:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for snapshot")
	}

	// Consume the delta event.
	select {
	case <-respCh:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for events")
	}

	// Close the manager — should terminate the watcher.
	m.Close()

	select {
	case err := <-errCh:
		if err != nil && err != ErrClosed {
			t.Fatalf("expected nil or ErrClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for watcher to exit after manager close")
	}
}

func TestManagerDefaultCapacity(t *testing.T) {
	m := NewManager(ManagerOptions{})
	defer m.Close()

	if m.opts.RingBufferCapacity != DefaultRingBufferCapacity {
		t.Fatalf("expected default capacity %d, got %d",
			DefaultRingBufferCapacity, m.opts.RingBufferCapacity)
	}
}
