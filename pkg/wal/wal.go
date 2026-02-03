package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrClosed is returned when operations are attempted on a closed WAL.
	ErrClosed = errors.New("wal: closed")
)

const (
	// walFilePrefix is the prefix for WAL segment files.
	walFilePrefix = "wal-"
	// walFileSuffix is the suffix for WAL segment files.
	walFileSuffix = ".log"
)

// WAL is a write-ahead log with configurable rotation and sync policies.
type WAL struct {
	dir  string
	opts Options

	mu      sync.Mutex
	file    *os.File
	encoder *Encoder

	// Segment tracking (for rotation)
	seq    uint64 // Current segment sequence number
	offset int64  // Current offset in segment

	closed bool
}

// Open opens or creates a WAL in the given directory.
// If the directory doesn't exist, it will be created.
// On open, all existing records are read for recovery.
func Open(dir string, opts Options) (*WAL, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory: %w", err)
	}

	w := &WAL{
		dir:  dir,
		opts: opts,
	}

	// Find existing segments
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		// No existing segments, create the first one
		if err := w.createSegment(0); err != nil {
			return nil, err
		}
	} else {
		// Open the last segment for appending
		lastSeg := segments[len(segments)-1]
		if err := w.openSegment(lastSeg); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// segmentInfo holds information about a WAL segment file.
type segmentInfo struct {
	seq  uint64
	path string
}

// listSegments returns all WAL segment files sorted by sequence number.
func (w *WAL) listSegments() ([]segmentInfo, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to read directory: %w", err)
	}

	var segments []segmentInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, walFilePrefix) || !strings.HasSuffix(name, walFileSuffix) {
			continue
		}

		// Parse sequence number
		seqStr := strings.TrimSuffix(strings.TrimPrefix(name, walFilePrefix), walFileSuffix)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue // Skip malformed files
		}

		segments = append(segments, segmentInfo{
			seq:  seq,
			path: filepath.Join(w.dir, name),
		})
	}

	// Sort by sequence number
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].seq < segments[j].seq
	})

	return segments, nil
}

// segmentPath returns the path for a segment with the given sequence number.
func (w *WAL) segmentPath(seq uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s%016d%s", walFilePrefix, seq, walFileSuffix))
}

// createSegment creates a new segment file.
func (w *WAL) createSegment(seq uint64) error {
	path := w.segmentPath(seq)

	// Create the file
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to create segment: %w", err)
	}

	// Preallocate if configured
	if w.opts.PreallocSize > 0 {
		if err := preallocate(f, w.opts.PreallocSize); err != nil {
			f.Close()
			return fmt.Errorf("wal: failed to preallocate: %w", err)
		}
	}

	w.file = f
	w.encoder = NewEncoder(f, 0, w.opts.BufferSize)
	w.seq = seq
	w.offset = 0

	return nil
}

// openSegment opens an existing segment for appending.
func (w *WAL) openSegment(seg segmentInfo) error {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to open segment: %w", err)
	}

	// Seek to end to get current offset
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek: %w", err)
	}

	// We need to find the actual end of valid data (not preallocated zeros)
	// Read from the beginning to find the last valid record
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek to start: %w", err)
	}

	decoder := NewDecoder(f)
	var rec Record
	for {
		err := decoder.Decode(&rec)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// End of valid data
				break
			}
			// Corruption - for now, we stop at the last valid record
			break
		}
	}

	// Seek to the end of valid data
	validOffset := decoder.LastOffset()
	if _, err := f.Seek(validOffset, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek to valid offset: %w", err)
	}

	// Truncate to remove any partial/corrupted data at the end
	if validOffset < offset {
		if err := f.Truncate(validOffset); err != nil {
			f.Close()
			return fmt.Errorf("wal: failed to truncate: %w", err)
		}
	}

	w.file = f
	w.encoder = NewEncoder(f, int(validOffset), w.opts.BufferSize)
	w.seq = seg.seq
	w.offset = validOffset

	return nil
}

// Append writes a record to the WAL.
// Returns the offset where the record was written.
// If SyncPolicy is SyncOnAppend, the record is flushed and fsynced before returning.
func (w *WAL) Append(rec *Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrClosed
	}

	// Check if we need to rotate
	if w.opts.SegmentSize > 0 && w.offset >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	// Encode the record
	offset, err := w.encoder.Encode(rec)
	if err != nil {
		return 0, fmt.Errorf("wal: failed to encode: %w", err)
	}

	// Update our offset tracking
	w.offset = w.encoder.Offset()

	// Sync if configured
	if w.opts.SyncPolicy == SyncOnAppend {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
	}

	// Return combined offset (seq:offset)
	// For single-file mode (no rotation), seq is always 0
	return uint64(offset), nil
}

// rotate creates a new segment when the current one is full.
func (w *WAL) rotate() error {
	// Flush and sync current segment
	if err := w.syncLocked(); err != nil {
		return err
	}

	// Close current segment
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close segment: %w", err)
	}

	// Create new segment
	return w.createSegment(w.seq + 1)
}

// Sync flushes buffered data and fsyncs to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	return w.syncLocked()
}

// syncLocked performs sync while holding the lock.
func (w *WAL) syncLocked() error {
	// Flush encoder buffer
	if err := w.encoder.Flush(); err != nil {
		return fmt.Errorf("wal: failed to flush: %w", err)
	}

	// Fsync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}

	return nil
}

// ReadAll reads all records from the WAL (for recovery).
// This reads all segments in order.
func (w *WAL) ReadAll() ([]*Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrClosed
	}

	// Flush any buffered data first
	if err := w.encoder.Flush(); err != nil {
		return nil, fmt.Errorf("wal: failed to flush before read: %w", err)
	}

	// List all segments
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var records []*Record
	for _, seg := range segments {
		segRecords, err := w.readSegment(seg.path)
		if err != nil {
			return nil, err
		}
		records = append(records, segRecords...)
	}

	return records, nil
}

// readSegment reads all records from a segment file.
func (w *WAL) readSegment(path string) ([]*Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open segment for reading: %w", err)
	}
	defer f.Close()

	decoder := NewDecoder(f)
	var records []*Record

	for {
		rec := &Record{}
		err := decoder.Decode(rec)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// Partial write at end - stop reading
				break
			}
			return nil, fmt.Errorf("wal: failed to decode record: %w", err)
		}
		records = append(records, rec)
	}

	return records, nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	// Flush and sync
	if err := w.encoder.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: failed to flush on close: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: failed to sync on close: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close file: %w", err)
	}

	return nil
}

// preallocate preallocates space in the file.
func preallocate(f *os.File, size int64) error {
	// Use Truncate for simple preallocation
	// On Linux, we could use fallocate for better performance
	return f.Truncate(size)
}
