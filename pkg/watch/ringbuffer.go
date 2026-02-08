package watch

import (
	"errors"
	"sync"
)

var (
	// ErrExpired is returned when the requested seqNum has already been
	// evicted from the ring buffer. The client must re-snapshot.
	ErrExpired = errors.New("watch: requested seqNum has expired from ring buffer")

	// ErrClosed is returned when operating on a closed ring buffer.
	ErrClosed = errors.New("watch: closed")
)

// Event is a single mutation event stored in the ring buffer.
// These are produced by TypeStore on each Create/Update/Delete and
// consumed by watchers during the delta phase.
type Event struct {
	// SeqNum is the storage-level sequence number of this mutation.
	SeqNum uint64

	// Operation is one of CREATE, UPDATE, DELETE.
	Operation Operation

	// PrimaryKey is the resource's primary key.
	PrimaryKey []byte

	// Data is the resource payload (nil for DELETE).
	Data []byte
}

// Operation enumerates the mutation types.
type Operation int

const (
	OpCreate Operation = 1
	OpUpdate Operation = 2
	OpDelete Operation = 3
)

// RingBuffer is a fixed-capacity circular buffer of watch events, indexed by
// storage sequence number. It supports concurrent reads from multiple watchers
// and serialized writes from the TypeStore.
//
// Design:
//   - Fixed-size array of Event values (not pointers, for cache locality).
//   - head points to the next write position (wraps around).
//   - count tracks how many entries are currently valid.
//   - Subscribers are notified via buffered channels on each Append.
//   - ReadFrom(seqNum) returns all events with SeqNum >= seqNum, or ErrExpired
//     if the oldest event has SeqNum > seqNum.
type RingBuffer struct {
	mu     sync.RWMutex
	buf    []Event
	cap    int
	head   int // next write position
	count  int // number of valid entries
	closed bool

	// subscribers receive notifications on every Append.
	// Each subscriber channel is buffered to avoid blocking the write path.
	subscribers map[uint64]chan struct{}
	nextSubID   uint64
}

// NewRingBuffer creates a ring buffer with the given capacity.
// Capacity must be > 0.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 1024
	}
	return &RingBuffer{
		buf:         make([]Event, capacity),
		cap:         capacity,
		subscribers: make(map[uint64]chan struct{}),
	}
}

// Append adds an event to the ring buffer and notifies all subscribers.
// If the buffer is full, the oldest entry is overwritten.
func (rb *RingBuffer) Append(event Event) {
	rb.mu.Lock()
	// Copy byte slices to avoid caller mutations affecting stored events.
	event.PrimaryKey = copyBytes(event.PrimaryKey)
	event.Data = copyBytes(event.Data)
	rb.buf[rb.head] = event
	rb.head = (rb.head + 1) % rb.cap
	if rb.count < rb.cap {
		rb.count++
	}

	// Snapshot subscriber channels before unlocking to avoid
	// holding the lock during channel sends.
	subs := make([]chan struct{}, 0, len(rb.subscribers))
	for _, ch := range rb.subscribers {
		subs = append(subs, ch)
	}
	rb.mu.Unlock()

	// Notify subscribers (non-blocking send on buffered channels).
	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
			// Subscriber hasn't consumed the previous notification yet.
			// That's fine — they'll see all buffered events on next ReadFrom.
		}
	}
}

// ReadFrom returns all events with SeqNum >= fromSeqNum.
// Returns ErrExpired if fromSeqNum is older than the oldest event in the buffer.
// Returns an empty slice (no error) if fromSeqNum > newest event.
func (rb *RingBuffer) ReadFrom(fromSeqNum uint64) ([]Event, error) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.closed {
		return nil, ErrClosed
	}
	if rb.count == 0 {
		return nil, nil
	}

	oldest := rb.oldestIndex()
	oldestSeq := rb.buf[oldest].SeqNum

	if fromSeqNum < oldestSeq {
		return nil, ErrExpired
	}

	// Find the starting position by scanning from oldest.
	var events []Event
	for i := 0; i < rb.count; i++ {
		idx := (oldest + i) % rb.cap
		if rb.buf[idx].SeqNum >= fromSeqNum {
			// Copy the event to avoid data races if the buffer slot is overwritten.
			e := rb.buf[idx]
			e.PrimaryKey = copyBytes(e.PrimaryKey)
			e.Data = copyBytes(e.Data)
			events = append(events, e)
		}
	}

	return events, nil
}

// OldestSeqNum returns the sequence number of the oldest event in the buffer.
// Returns 0 if the buffer is empty.
func (rb *RingBuffer) OldestSeqNum() uint64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return 0
	}
	return rb.buf[rb.oldestIndex()].SeqNum
}

// NewestSeqNum returns the sequence number of the newest event in the buffer.
// Returns 0 if the buffer is empty.
func (rb *RingBuffer) NewestSeqNum() uint64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return 0
	}
	newest := (rb.head - 1 + rb.cap) % rb.cap
	return rb.buf[newest].SeqNum
}

// Len returns the number of events currently in the buffer.
func (rb *RingBuffer) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count
}

// Subscribe registers a subscriber and returns a channel that receives a
// notification (empty struct) on every Append. The channel is buffered with
// capacity 1 to avoid blocking the write path.
//
// Call Unsubscribe with the returned ID when done.
func (rb *RingBuffer) Subscribe() (id uint64, ch <-chan struct{}) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	notifyCh := make(chan struct{}, 1)
	id = rb.nextSubID
	rb.nextSubID++
	rb.subscribers[id] = notifyCh
	return id, notifyCh
}

// Unsubscribe removes a subscriber by ID and closes the notification channel.
func (rb *RingBuffer) Unsubscribe(id uint64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if ch, ok := rb.subscribers[id]; ok {
		close(ch)
		delete(rb.subscribers, id)
	}
}

// Close marks the ring buffer as closed and closes all subscriber channels.
func (rb *RingBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.closed = true
	for id, ch := range rb.subscribers {
		close(ch)
		delete(rb.subscribers, id)
	}
}

// oldestIndex returns the index of the oldest entry. Caller must hold at least RLock.
func (rb *RingBuffer) oldestIndex() int {
	if rb.count < rb.cap {
		return 0
	}
	return rb.head // when full, head points to the oldest (about to be overwritten)
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
