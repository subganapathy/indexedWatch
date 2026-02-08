package watch

import (
	"sync"
	"testing"
	"time"
)

func TestRingBufferAppendAndReadFrom(t *testing.T) {
	rb := NewRingBuffer(8)

	// Append 5 events.
	for i := uint64(1); i <= 5; i++ {
		rb.Append(Event{
			SeqNum:     i,
			Operation:  OpCreate,
			PrimaryKey: []byte{byte(i)},
			Data:       []byte{byte(i + 100)},
		})
	}

	if rb.Len() != 5 {
		t.Fatalf("len: expected 5, got %d", rb.Len())
	}
	if rb.OldestSeqNum() != 1 {
		t.Fatalf("oldest: expected 1, got %d", rb.OldestSeqNum())
	}
	if rb.NewestSeqNum() != 5 {
		t.Fatalf("newest: expected 5, got %d", rb.NewestSeqNum())
	}

	// Read from seqNum 3.
	events, err := rb.ReadFrom(3)
	if err != nil {
		t.Fatalf("ReadFrom(3): %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	for i, e := range events {
		expected := uint64(3 + i)
		if e.SeqNum != expected {
			t.Fatalf("event[%d] seqNum: expected %d, got %d", i, expected, e.SeqNum)
		}
	}

	// Read all.
	events, err = rb.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom(1): %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
}

func TestRingBufferWraparound(t *testing.T) {
	rb := NewRingBuffer(4)

	// Append 7 events into a buffer of capacity 4.
	for i := uint64(1); i <= 7; i++ {
		rb.Append(Event{SeqNum: i, Operation: OpCreate})
	}

	if rb.Len() != 4 {
		t.Fatalf("len: expected 4, got %d", rb.Len())
	}
	if rb.OldestSeqNum() != 4 {
		t.Fatalf("oldest: expected 4, got %d", rb.OldestSeqNum())
	}
	if rb.NewestSeqNum() != 7 {
		t.Fatalf("newest: expected 7, got %d", rb.NewestSeqNum())
	}

	// Reading seqNum 3 should be expired (oldest is 4).
	_, err := rb.ReadFrom(3)
	if err != ErrExpired {
		t.Fatalf("ReadFrom(3): expected ErrExpired, got %v", err)
	}

	// Reading from 4 should work.
	events, err := rb.ReadFrom(4)
	if err != nil {
		t.Fatalf("ReadFrom(4): %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}
	for i, e := range events {
		expected := uint64(4 + i)
		if e.SeqNum != expected {
			t.Fatalf("event[%d]: expected seqNum %d, got %d", i, expected, e.SeqNum)
		}
	}
}

func TestRingBufferEmpty(t *testing.T) {
	rb := NewRingBuffer(4)

	if rb.Len() != 0 {
		t.Fatalf("len: expected 0, got %d", rb.Len())
	}
	if rb.OldestSeqNum() != 0 {
		t.Fatalf("oldest: expected 0, got %d", rb.OldestSeqNum())
	}
	if rb.NewestSeqNum() != 0 {
		t.Fatalf("newest: expected 0, got %d", rb.NewestSeqNum())
	}

	// ReadFrom on empty buffer should return nil, nil.
	events, err := rb.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom on empty: %v", err)
	}
	if events != nil {
		t.Fatalf("expected nil events, got %v", events)
	}
}

func TestRingBufferReadFromFuture(t *testing.T) {
	rb := NewRingBuffer(4)

	rb.Append(Event{SeqNum: 1, Operation: OpCreate})
	rb.Append(Event{SeqNum: 2, Operation: OpUpdate})

	// Reading from a future seqNum should return empty, no error.
	events, err := rb.ReadFrom(100)
	if err != nil {
		t.Fatalf("ReadFrom(100): %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestRingBufferDataIsolation(t *testing.T) {
	rb := NewRingBuffer(4)

	pk := []byte("original")
	rb.Append(Event{
		SeqNum:     1,
		Operation:  OpCreate,
		PrimaryKey: pk,
		Data:       []byte("data"),
	})

	// Mutate the original slice.
	pk[0] = 'X'

	// ReadFrom should return a copy, not the mutated original.
	events, _ := rb.ReadFrom(1)
	if string(events[0].PrimaryKey) == "Xriginal" {
		t.Fatal("ReadFrom returned reference to original PK slice (not a copy)")
	}
}

func TestRingBufferSubscribe(t *testing.T) {
	rb := NewRingBuffer(8)

	id, ch := rb.Subscribe()
	defer rb.Unsubscribe(id)

	// Append should trigger notification.
	rb.Append(Event{SeqNum: 1, Operation: OpCreate})

	select {
	case <-ch:
		// OK — got notification.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for subscriber notification")
	}

	// Consume events.
	events, _ := rb.ReadFrom(1)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestRingBufferMultipleSubscribers(t *testing.T) {
	rb := NewRingBuffer(8)

	id1, ch1 := rb.Subscribe()
	id2, ch2 := rb.Subscribe()
	defer rb.Unsubscribe(id1)
	defer rb.Unsubscribe(id2)

	rb.Append(Event{SeqNum: 1, Operation: OpCreate})

	// Both subscribers should be notified.
	for _, ch := range []<-chan struct{}{ch1, ch2} {
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for subscriber notification")
		}
	}
}

func TestRingBufferUnsubscribe(t *testing.T) {
	rb := NewRingBuffer(8)

	id, ch := rb.Subscribe()
	rb.Unsubscribe(id)

	// Channel should be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout — channel not closed after Unsubscribe")
	}
}

func TestRingBufferClose(t *testing.T) {
	rb := NewRingBuffer(4)
	rb.Append(Event{SeqNum: 1})

	id, ch := rb.Subscribe()
	_ = id

	rb.Close()

	// ReadFrom should return ErrClosed.
	_, err := rb.ReadFrom(1)
	if err != ErrClosed {
		t.Fatalf("ReadFrom after Close: expected ErrClosed, got %v", err)
	}

	// Subscriber channel should be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after Close")
		}
	default:
		t.Fatal("channel not closed after Close")
	}
}

func TestRingBufferConcurrentAppendAndRead(t *testing.T) {
	rb := NewRingBuffer(256)
	const writers = 4
	const eventsPerWriter = 100

	var wg sync.WaitGroup

	// Concurrent writers.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < eventsPerWriter; i++ {
				seq := uint64(writerID*eventsPerWriter + i + 1)
				rb.Append(Event{
					SeqNum:     seq,
					Operation:  OpCreate,
					PrimaryKey: []byte{byte(writerID), byte(i)},
				})
			}
		}(w)
	}

	// Concurrent readers.
	for r := 0; r < writers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < eventsPerWriter; i++ {
				rb.ReadFrom(1) // may get ErrExpired, that's OK
				rb.OldestSeqNum()
				rb.NewestSeqNum()
				rb.Len()
			}
		}()
	}

	wg.Wait()

	// Buffer should have some events (may have wrapped around).
	if rb.Len() == 0 {
		t.Fatal("expected events in buffer after concurrent writes")
	}
}

func TestRingBufferExactCapacity(t *testing.T) {
	rb := NewRingBuffer(4)

	// Fill exactly to capacity.
	for i := uint64(1); i <= 4; i++ {
		rb.Append(Event{SeqNum: i})
	}

	if rb.Len() != 4 {
		t.Fatalf("len: expected 4, got %d", rb.Len())
	}
	if rb.OldestSeqNum() != 1 {
		t.Fatalf("oldest: expected 1, got %d", rb.OldestSeqNum())
	}

	// One more should evict seqNum 1.
	rb.Append(Event{SeqNum: 5})

	if rb.OldestSeqNum() != 2 {
		t.Fatalf("oldest after overflow: expected 2, got %d", rb.OldestSeqNum())
	}
	if rb.NewestSeqNum() != 5 {
		t.Fatalf("newest after overflow: expected 5, got %d", rb.NewestSeqNum())
	}
}

func TestRingBufferDefaultCapacity(t *testing.T) {
	rb := NewRingBuffer(0)
	if rb.cap != 1024 {
		t.Fatalf("expected default capacity 1024, got %d", rb.cap)
	}

	rb2 := NewRingBuffer(-5)
	if rb2.cap != 1024 {
		t.Fatalf("expected default capacity 1024, got %d", rb2.cap)
	}
}
