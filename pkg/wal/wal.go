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
	walFilePrefix = "wal-"
	walFileSuffix = ".log"
)

// WAL is a write-ahead log with configurable rotation and sync policies.
//
// The WAL owns the Record envelope: callers provide opaque data bytes via
// Append, and the WAL wraps them as DataType records with chained CRC.
// WAL-level record types (CrcType, SnapshotType) are written internally.
//
// Sync always goes through the group commit mechanism (syncGroup),
// regardless of SyncPolicy. SyncOnAppend calls Sync() automatically
// after each Append; SyncManual requires the caller to call Sync()
// explicitly. Both policies benefit from group commit when multiple
// goroutines sync concurrently.
type WAL struct {
	dir  string
	opts Options

	mu        sync.Mutex
	file      *os.File
	encoder   *Encoder
	syncGroup *syncGroup

	seq    uint64 // Current segment sequence number
	offset int64  // Current offset in segment

	closed bool
}

// --- Exported methods ---

// Open opens or creates a WAL in the given directory.
// If the directory doesn't exist, it will be created.
func Open(dir string, opts Options) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory: %w", err)
	}

	w := &WAL{
		dir:  dir,
		opts: opts,
	}

	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		if err := w.createSegment(0, 0); err != nil {
			return nil, err
		}
	} else {
		lastSeg := segments[len(segments)-1]
		if err := w.openSegment(lastSeg); err != nil {
			return nil, err
		}
	}

	// syncGroup's syncFunc acquires w.mu internally — required for
	// correctness so that flush+fsync is atomic w.r.t. Append/rotate.
	w.syncGroup = newSyncGroup(func() error {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.closed {
			return ErrClosed
		}
		return w.syncLocked()
	})

	return w, nil
}

// Append writes opaque data to the WAL as a DataType record.
// The WAL owns the Record envelope — CRC is set by the encoder's chain.
//
// The lock is released after encoding so that Sync() (called automatically
// for SyncOnAppend, or explicitly for SyncManual) goes through the unified
// group commit path. This allows concurrent Appends to proceed while
// a sync is in flight.
func (w *WAL) Append(data []byte) (uint64, error) {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return 0, ErrClosed
	}

	if w.opts.SegmentSize > 0 && w.offset >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	}

	rec := &walpb.Record{
		Type: walpb.DataType,
		Data: data,
	}

	offset, err := w.encoder.Encode(rec)
	if err != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: failed to encode: %w", err)
	}

	// Encoder sets rec.Crc via the CRC chain. Verify it was actually set
	// (guards against encoder bugs silently producing uncheckable records).
	if rec.Crc == 0 && len(data) > 0 {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: encoder did not set CRC")
	}

	w.offset = w.encoder.Offset()
	w.mu.Unlock()

	if w.opts.SyncPolicy == SyncOnAppend {
		if err := w.Sync(); err != nil {
			return 0, err
		}
	}

	return uint64(offset), nil
}

// SaveSnapshot writes a SnapshotType marker record to the WAL.
// The marker records the WAL offset and application-defined metadata.
// Actual snapshot data (e.g., LSM SSTables) lives outside the WAL.
// This always syncs to ensure the marker is durable.
func (w *WAL) SaveSnapshot(snap *walpb.Snapshot) error {
	if snap == nil {
		return fmt.Errorf("wal: snapshot must not be nil")
	}

	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}

	// Check rotation before writing the snapshot marker.
	if w.opts.SegmentSize > 0 && w.offset >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			w.mu.Unlock()
			return err
		}
	}

	snapData, err := proto.Marshal(snap)
	if err != nil {
		w.mu.Unlock()
		return fmt.Errorf("wal: failed to marshal snapshot: %w", err)
	}

	rec := &walpb.Record{
		Type: walpb.SnapshotType,
		Data: snapData,
	}

	if _, err := w.encoder.Encode(rec); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("wal: failed to encode snapshot: %w", err)
	}

	w.offset = w.encoder.Offset()
	w.mu.Unlock()

	// Always sync snapshot markers through the unified path.
	return w.Sync()
}

// Sync flushes buffered data and fsyncs to disk.
// All sync operations go through the group commit mechanism:
// concurrent callers share a single fdatasync via leader-follower.
func (w *WAL) Sync() error {
	return w.syncGroup.sync()
}

// ReadAll reads all records from the WAL (for recovery).
// Returns only DataType payloads. CrcType and SnapshotType records
// are validated by the decoder but filtered out.
func (w *WAL) ReadAll() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrClosed
	}

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

// --- Unexported methods ---

// segmentInfo holds information about a WAL segment file.
type segmentInfo struct {
	seq  uint64
	path string
}

// segmentPath returns the path for a segment with the given sequence number.
func (w *WAL) segmentPath(seq uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s%016d%s", walFilePrefix, seq, walFileSuffix))
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

// createSegment creates a new segment file.
// prevCrc is the CRC chain value from the previous segment (0 for the first).
// Writes a CrcType checkpoint as the first record, then fsyncs to ensure
// the segment header is durable before any data records.
//
// WAL state (w.file, w.encoder, w.seq, w.offset) is only updated after
// the CRC checkpoint and sync succeed — a failure leaves the WAL in its
// previous state so the caller can handle it cleanly.
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
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return fmt.Errorf("wal: failed to seek after preallocate: %w", err)
		}
	}

	// Create encoder on the new file (local, not yet assigned to w).
	enc := NewEncoder(f, prevCrc, 0, w.opts.BufferSize)

	// Write CRC checkpoint as the first record.
	rec := &walpb.Record{Type: walpb.CrcType, Crc: prevCrc}
	if _, err := enc.Encode(rec); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to write CRC checkpoint: %w", err)
	}

	// Flush + fsync to ensure the checkpoint is durable.
	if err := enc.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to flush new segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to sync new segment: %w", err)
	}

	// Success — commit state.
	w.file = f
	w.encoder = enc
	w.seq = seq
	w.offset = enc.Offset()

	return nil
}

// openSegment opens an existing segment for appending.
// It reads all records to find the valid end, zeros trailing garbage
// (preserving preallocated space for torn write detection), then seeds
// the encoder with the decoder's final CRC for continuous chaining.
//
// Returns an error if the segment contains corruption (CRC mismatch,
// malformed records). Torn writes (io.ErrUnexpectedEOF) are treated
// as recoverable — the partial tail is zeroed.
func (w *WAL) openSegment(seg segmentInfo) error {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to open segment: %w", err)
	}

	// Read from the beginning to find the last valid record.
	decoder := NewDecoder(f)
	var rec walpb.Record
	for {
		err := decoder.Decode(&rec)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // Normal end or torn write — recoverable.
			}
			// Corruption (CRC mismatch, malformed protobuf) — not recoverable.
			f.Close()
			return fmt.Errorf("wal: corrupt segment %s: %w", seg.path, err)
		}
	}

	// Seek to the end of valid data.
	validOffset := decoder.LastOffset()
	if _, err := f.Seek(validOffset, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek to valid offset: %w", err)
	}

	// Zero from valid end to file end. This preserves the file size
	// (and preallocated space) while ensuring partial/corrupted data
	// is replaced with zeros — required for torn write detection.
	if err := zeroToEnd(f); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to zero trailing data: %w", err)
	}

	// Seek back to valid end for writing.
	if _, err := f.Seek(validOffset, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("wal: failed to seek after zero: %w", err)
	}

	w.file = f
	// Seed the encoder with the decoder's final CRC — continuous chain.
	// Only the page offset modulo walPageBytes matters for alignment.
	w.encoder = NewEncoder(f, decoder.LastCRC(), int(validOffset%int64(walPageBytes)), w.opts.BufferSize)
	w.seq = seg.seq
	w.offset = validOffset

	return nil
}

// rotate creates a new segment when the current one is full.
// Bridges the CRC chain by passing the current encoder's CRC to the
// new segment's CrcType checkpoint.
func (w *WAL) rotate() error {
	if err := w.syncLocked(); err != nil {
		return err
	}

	prevCrc := w.encoder.CRC()

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close segment: %w", err)
	}

	return w.createSegment(w.seq+1, prevCrc)
}

// syncLocked flushes the encoder buffer and fsyncs the file.
// Caller must hold w.mu.
func (w *WAL) syncLocked() error {
	if err := w.encoder.Flush(); err != nil {
		return fmt.Errorf("wal: failed to flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}
	return nil
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

		if rec.Type == walpb.DataType {
			d := make([]byte, len(rec.Data))
			copy(d, rec.Data)
			data = append(data, d)
		}
	}

	return data, nil
}

// zeroToEnd zeros the file from the current position to the end.
// This preserves the file size (preallocated space) while clearing
// partial writes — required for torn write detection (zero sectors
// indicate "no data written here").
func zeroToEnd(f *os.File) error {
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if pos >= end {
		return nil
	}
	// Seek back to where we were.
	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	// Write zeros in chunks.
	zeros := make([]byte, 4096)
	remaining := end - pos
	for remaining > 0 {
		n := min(int64(len(zeros)), remaining)
		if _, err := f.Write(zeros[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	return nil
}

// preallocate preallocates space in the file.
func preallocate(f *os.File, size int64) error {
	return f.Truncate(size)
}
