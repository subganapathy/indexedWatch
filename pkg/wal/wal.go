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

	"github.com/subganapathy/indexedwatch/pkg/wal/walpb"
	"google.golang.org/protobuf/proto"
)

const (
	// walFilePrefix is the prefix for WAL segment files.
	walFilePrefix = "wal-"
	// walFileSuffix is the suffix for WAL segment files.
	walFileSuffix = ".log"
)

// WAL is a write-ahead log with configurable rotation and sync policies.
//
// The WAL owns the Record envelope: callers provide opaque data bytes via
// Append, and the WAL wraps them as DataType records with chained CRC.
// WAL-level record types (CrcType, SnapshotType) are written internally.
type WAL struct {
	dir  string
	opts Options

	mu        sync.Mutex
	file      *os.File
	encoder   *Encoder
	syncGroup *syncGroup

	// Segment tracking (for rotation).
	seq    uint64 // Current segment sequence number
	offset int64  // Current offset in segment

	closed bool
}

// Open opens or creates a WAL in the given directory.
// If the directory doesn't exist, it will be created.
// On open, all existing records are read for recovery.
func Open(dir string, opts Options) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory: %w", err)
	}

	w := &WAL{
		dir:  dir,
		opts: opts,
	}

	// Find existing segments.
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		// No existing segments — create the first one with CRC seed 0.
		if err := w.createSegment(0, 0); err != nil {
			return nil, err
		}
	} else {
		// Open the last segment for appending.
		lastSeg := segments[len(segments)-1]
		if err := w.openSegment(lastSeg); err != nil {
			return nil, err
		}
	}

	// Set up group commit for SyncManual mode.
	w.syncGroup = newSyncGroup(w.syncLocked)

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

		seqStr := strings.TrimSuffix(strings.TrimPrefix(name, walFilePrefix), walFileSuffix)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}

		segments = append(segments, segmentInfo{
			seq:  seq,
			path: filepath.Join(w.dir, name),
		})
	}

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
// prevCrc is the CRC chain value from the previous segment (0 for the first segment).
// A CrcType checkpoint record is written as the first record of every segment
// to bridge the CRC chain across segment boundaries.
func (w *WAL) createSegment(seq uint64, prevCrc uint32) error {
	path := w.segmentPath(seq)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to create segment: %w", err)
	}

	// Preallocate if configured.
	if w.opts.PreallocSize > 0 {
		if err := preallocate(f, w.opts.PreallocSize); err != nil {
			f.Close()
			return fmt.Errorf("wal: failed to preallocate: %w", err)
		}
		// Seek back to start after preallocation.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return fmt.Errorf("wal: failed to seek after preallocate: %w", err)
		}
	}

	w.file = f
	w.encoder = NewEncoder(f, prevCrc, 0, w.opts.BufferSize)
	w.seq = seq
	w.offset = 0

	// Write CRC checkpoint as the first record.
	if err := w.saveCrc(prevCrc); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to write CRC checkpoint: %w", err)
	}

	return nil
}

// saveCrc writes a CrcType record that checkpoints the CRC chain.
func (w *WAL) saveCrc(prevCrc uint32) error {
	rec := &walpb.Record{Type: walpb.CrcType, Crc: prevCrc}
	_, err := w.encoder.Encode(rec)
	if err != nil {
		return err
	}
	w.offset = w.encoder.Offset()
	return nil
}

// openSegment opens an existing segment for appending.
// It reads all records to find the valid end, then seeds the encoder
// with the decoder's final CRC for continuous chaining.
func (w *WAL) openSegment(seg segmentInfo) error {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to open segment: %w", err)
	}

	// Get file size for potential truncation later.
	fileSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek: %w", err)
	}

	// Read from the beginning to find the last valid record.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek to start: %w", err)
	}

	decoder := NewDecoder(f)
	var rec walpb.Record
	for {
		err := decoder.Decode(&rec)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			// Corruption — stop at the last valid record.
			break
		}
	}

	// Seek to the end of valid data.
	validOffset := decoder.LastOffset()
	if _, err := f.Seek(validOffset, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek to valid offset: %w", err)
	}

	// Truncate to remove any partial/corrupted data at the end.
	if validOffset < fileSize {
		if err := f.Truncate(validOffset); err != nil {
			f.Close()
			return fmt.Errorf("wal: failed to truncate: %w", err)
		}
	}

	w.file = f
	// Seed the encoder with the decoder's final CRC — continuous chain.
	w.encoder = NewEncoder(f, decoder.LastCRC(), int(validOffset), w.opts.BufferSize)
	w.seq = seg.seq
	w.offset = validOffset

	return nil
}

// Append writes opaque data to the WAL as a DataType record.
// The WAL owns the Record envelope and CRC computation.
// Returns the offset where the record was written.
// If SyncPolicy is SyncOnAppend, the record is flushed and fsynced before returning.
func (w *WAL) Append(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrClosed
	}

	// Check if we need to rotate.
	if w.opts.SegmentSize > 0 && w.offset >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	// Build the record — WAL owns the envelope.
	rec := &walpb.Record{
		Type: walpb.DataType,
		Data: data,
	}

	// Encode the record (CRC is set by the encoder's chain).
	offset, err := w.encoder.Encode(rec)
	if err != nil {
		return 0, fmt.Errorf("wal: failed to encode: %w", err)
	}

	w.offset = w.encoder.Offset()

	// Sync if configured.
	if w.opts.SyncPolicy == SyncOnAppend {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
	}

	return uint64(offset), nil
}

// SaveSnapshot writes a SnapshotType marker record to the WAL.
// The snapshot marker records the WAL offset at which the snapshot was taken.
// Actual snapshot data (e.g., LSM SSTables) lives outside the WAL.
// This always syncs to ensure the marker is durable.
func (w *WAL) SaveSnapshot(snap *walpb.Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	snapData, err := proto.Marshal(snap)
	if err != nil {
		return fmt.Errorf("wal: failed to marshal snapshot: %w", err)
	}

	rec := &walpb.Record{
		Type: walpb.SnapshotType,
		Data: snapData,
	}

	if _, err := w.encoder.Encode(rec); err != nil {
		return fmt.Errorf("wal: failed to encode snapshot: %w", err)
	}

	w.offset = w.encoder.Offset()

	// Always sync snapshot markers.
	return w.syncLocked()
}

// rotate creates a new segment when the current one is full.
// It bridges the CRC chain by passing the current encoder's CRC
// to the new segment's CrcType checkpoint.
func (w *WAL) rotate() error {
	// Flush and sync current segment.
	if err := w.syncLocked(); err != nil {
		return err
	}

	// Capture CRC before closing the encoder.
	prevCrc := w.encoder.CRC()

	// Close current segment.
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close segment: %w", err)
	}

	// Create new segment with CRC bridge.
	return w.createSegment(w.seq+1, prevCrc)
}

// Sync flushes buffered data and fsyncs to disk.
// When SyncPolicy is SyncManual, concurrent callers share a single
// fdatasync via the group commit mechanism.
func (w *WAL) Sync() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.mu.Unlock()

	if w.opts.SyncPolicy == SyncManual {
		return w.syncGroup.sync()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

// syncLocked performs sync while holding the lock.
func (w *WAL) syncLocked() error {
	if err := w.encoder.Flush(); err != nil {
		return fmt.Errorf("wal: failed to flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}
	return nil
}

// ReadAll reads all records from the WAL (for recovery).
// This reads all segments in order and returns only DataType records.
// CrcType and SnapshotType records are validated but filtered out.
func (w *WAL) ReadAll() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrClosed
	}

	// Flush any buffered data first.
	if err := w.encoder.Flush(); err != nil {
		return nil, fmt.Errorf("wal: failed to flush before read: %w", err)
	}

	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var data [][]byte
	for _, seg := range segments {
		segData, err := w.readSegment(seg.path)
		if err != nil {
			return nil, err
		}
		data = append(data, segData...)
	}

	return data, nil
}

// readSegment reads all records from a segment file.
// Returns only the Data field of DataType records.
func (w *WAL) readSegment(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open segment for reading: %w", err)
	}
	defer f.Close()

	decoder := NewDecoder(f)
	var data [][]byte

	for {
		var rec walpb.Record
		err := decoder.Decode(&rec)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("wal: failed to decode record: %w", err)
		}

		// Only return DataType records to callers.
		if rec.Type == walpb.DataType {
			// Copy data so it doesn't reference the decoder's buffer.
			d := make([]byte, len(rec.Data))
			copy(d, rec.Data)
			data = append(data, d)
		}
	}

	return data, nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

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
	return f.Truncate(size)
}
