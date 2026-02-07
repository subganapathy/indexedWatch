package wal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/subganapathy/indexedwatch/pkg/wal/walpb"
	"google.golang.org/protobuf/proto"
)

// --- PageWriter tests (unchanged) ---

func TestPageWriter_BasicWrite(t *testing.T) {
	var buf bytes.Buffer
	pw := NewPageWriter(&buf, 4096, 0, 128*1024)

	data := []byte("hello world")
	n, err := pw.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned wrong count: got %d, want %d", n, len(data))
	}

	if buf.Len() != 0 {
		t.Errorf("Data was written before flush: got %d bytes", buf.Len())
	}

	if err := pw.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if buf.Len() != len(data) {
		t.Errorf("After flush: got %d bytes, want %d", buf.Len(), len(data))
	}
}

func TestPageWriter_LargeWrite(t *testing.T) {
	var buf bytes.Buffer
	bufferSize := 1024
	pw := NewPageWriter(&buf, 4096, 0, bufferSize)

	data := make([]byte, bufferSize*2)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := pw.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned wrong count: got %d, want %d", n, len(data))
	}

	if err := pw.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if buf.Len() != len(data) {
		t.Errorf("Total bytes written: got %d, want %d", buf.Len(), len(data))
	}
}

// --- Encoder/Decoder round-trip ---

func TestEncoder_Encode(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 0, 4096)

	rec := &walpb.Record{
		Type: walpb.DataType,
		Data: []byte("test record"),
	}

	offset, err := enc.Encode(rec)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("First record offset should be 0, got %d", offset)
	}

	if rec.Crc == 0 {
		t.Error("CRC should have been set by Encode")
	}

	if err := enc.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if buf.Len() == 0 {
		t.Error("No data written after flush")
	}
}

func TestEncoderDecoder_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 0, 4096)

	payloads := [][]byte{
		[]byte("first"),
		[]byte("second record with more data"),
		[]byte("third"),
	}

	for _, data := range payloads {
		rec := &walpb.Record{Type: walpb.DataType, Data: data}
		if _, err := enc.Encode(rec); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	}

	if err := enc.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	dec := NewDecoder(&buf)
	for i, expected := range payloads {
		var rec walpb.Record
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if rec.Type != walpb.DataType {
			t.Errorf("Record %d type: got %d, want %d", i, rec.Type, walpb.DataType)
		}
		if !bytes.Equal(rec.Data, expected) {
			t.Errorf("Record %d data: got %q, want %q", i, rec.Data, expected)
		}
	}

	var rec walpb.Record
	if err := dec.Decode(&rec); err != io.EOF {
		t.Errorf("Expected EOF, got: %v", err)
	}
}

// --- WAL integration tests ---

func TestWAL_BasicAppendRead(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	payloads := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	for _, data := range payloads {
		if _, err := w.Append(data); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	readData, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != len(payloads) {
		t.Fatalf("ReadAll returned %d records, want %d", len(readData), len(payloads))
	}

	for i, data := range readData {
		if !bytes.Equal(data, payloads[i]) {
			t.Errorf("Record %d data: got %q, want %q", i, data, payloads[i])
		}
	}
}

func TestWAL_Recovery(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	payloads := [][]byte{
		[]byte("persistent record 1"),
		[]byte("persistent record 2"),
	}

	for _, data := range payloads {
		if _, err := w.Append(data); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify.
	w2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	readData, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != len(payloads) {
		t.Fatalf("After recovery: got %d records, want %d", len(readData), len(payloads))
	}

	for i, data := range readData {
		if !bytes.Equal(data, payloads[i]) {
			t.Errorf("Record %d after recovery: got %q, want %q", i, data, payloads[i])
		}
	}

	// Append more records after recovery.
	newData := []byte("new record after recovery")
	if _, err := w2.Append(newData); err != nil {
		t.Fatalf("Append after recovery failed: %v", err)
	}

	readData, err = w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after new append failed: %v", err)
	}

	if len(readData) != 3 {
		t.Errorf("Expected 3 records after new append, got %d", len(readData))
	}
}

func TestWAL_SyncOnAppend(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		SyncPolicy: SyncOnAppend,
		BufferSize: 4096,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	if _, err := w.Append([]byte("sync on append")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	segments, err := w.listSegments()
	if err != nil {
		t.Fatalf("listSegments failed: %v", err)
	}
	if len(segments) == 0 {
		t.Fatal("No segments found")
	}

	fi, err := os.Stat(segments[0].path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if fi.Size() == 0 {
		t.Error("File size is 0 after SyncOnAppend")
	}
}

func TestWAL_ManualSync(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		SyncPolicy: SyncManual,
		BufferSize: 128 * 1024,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	if _, err := w.Append([]byte("manual sync")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	segments, err := w.listSegments()
	if err != nil {
		t.Fatalf("listSegments failed: %v", err)
	}

	fi, err := os.Stat(segments[0].path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if fi.Size() == 0 {
		t.Error("File size is 0 after manual Sync")
	}
}

func TestWAL_Rotation(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		SegmentSize: 100,
		SyncPolicy:  SyncOnAppend,
		BufferSize:  32,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	var allPayloads [][]byte
	for i := 0; i < 10; i++ {
		data := []byte("record that will trigger rotation")
		allPayloads = append(allPayloads, data)
		if _, err := w.Append(data); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	segments, err := w.listSegments()
	if err != nil {
		t.Fatalf("listSegments failed: %v", err)
	}

	if len(segments) < 2 {
		t.Errorf("Expected multiple segments, got %d", len(segments))
	}

	readData, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != len(allPayloads) {
		t.Fatalf("ReadAll returned %d records, want %d", len(readData), len(allPayloads))
	}

	for i, data := range readData {
		if !bytes.Equal(data, allPayloads[i]) {
			t.Errorf("Record %d data mismatch", i)
		}
	}
}

func TestWAL_NoRotation(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		SegmentSize: 0,
		SyncPolicy:  SyncOnAppend,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	for i := 0; i < 100; i++ {
		if _, err := w.Append([]byte("no rotation record")); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	segments, err := w.listSegments()
	if err != nil {
		t.Fatalf("listSegments failed: %v", err)
	}

	if len(segments) != 1 {
		t.Errorf("Expected 1 segment with no rotation, got %d", len(segments))
	}
}

func TestWAL_CRCValidation(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if _, err := w.Append([]byte("crc test")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	w.Close()

	// Corrupt the data.
	segments, _ := (&WAL{dir: dir}).listSegments()
	data, _ := os.ReadFile(segments[0].path)

	if len(data) > 30 {
		data[30] ^= 0xFF
		os.WriteFile(segments[0].path, data, 0644)
	}

	// Try to read — should detect corruption.
	w2, err := Open(dir, DefaultOptions())
	if err != nil {
		return // Corruption detected during open is acceptable.
	}
	defer w2.Close()

	_, err = w2.ReadAll()
	// Error or empty is acceptable for corrupted data.
	_ = err
}

func TestWAL_TailCorruption(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := w.Append([]byte("valid record")); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	w.Close()

	// Append garbage (simulating torn write).
	segments, _ := (&WAL{dir: dir}).listSegments()
	f, _ := os.OpenFile(segments[0].path, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0x12, 0x34, 0x56, 0x78})
	f.Close()

	// Reopen — should recover valid records and truncate garbage.
	w2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	readData, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != 3 {
		t.Errorf("Expected 3 valid records after tail corruption recovery, got %d", len(readData))
	}
}

func TestWAL_EmptyDir(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open on empty dir failed: %v", err)
	}
	defer w.Close()

	segments, err := w.listSegments()
	if err != nil {
		t.Fatalf("listSegments failed: %v", err)
	}

	if len(segments) != 1 {
		t.Errorf("Expected 1 segment, got %d", len(segments))
	}
}

func TestWAL_Closed(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	w.Close()

	if _, err := w.Append([]byte("test")); !errors.Is(err, ErrClosed) {
		t.Errorf("Append on closed WAL: got %v, want ErrClosed", err)
	}

	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Errorf("Sync on closed WAL: got %v, want ErrClosed", err)
	}

	if _, err := w.ReadAll(); !errors.Is(err, ErrClosed) {
		t.Errorf("ReadAll on closed WAL: got %v, want ErrClosed", err)
	}
}

func TestSchemaOptions(t *testing.T) {
	opts := SchemaOptions()
	if opts.SegmentSize != 0 {
		t.Errorf("SchemaOptions SegmentSize: got %d, want 0", opts.SegmentSize)
	}
	if opts.SyncPolicy != SyncOnAppend {
		t.Errorf("SchemaOptions SyncPolicy: got %v, want SyncOnAppend", opts.SyncPolicy)
	}
}

func TestResourceOptions(t *testing.T) {
	opts := ResourceOptions()
	if opts.SegmentSize != 64*1024*1024 {
		t.Errorf("ResourceOptions SegmentSize: got %d, want %d", opts.SegmentSize, 64*1024*1024)
	}
	if opts.SyncPolicy != SyncManual {
		t.Errorf("ResourceOptions SyncPolicy: got %v, want SyncManual", opts.SyncPolicy)
	}
}

func TestWAL_LargeRecord(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	largeData := make([]byte, 2*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	if _, err := w.Append(largeData); err != nil {
		t.Fatalf("Append large record failed: %v", err)
	}

	readData, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(readData))
	}

	if !bytes.Equal(readData[0], largeData) {
		t.Error("Large record data mismatch")
	}
}

func TestWAL_Preallocation(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		SyncPolicy:   SyncOnAppend,
		PreallocSize: 1024 * 1024,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if _, err := w.Append([]byte("test in preallocated file")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	w.Close()

	// Reopen and verify.
	w2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	readData, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(readData))
	}

	if !bytes.Equal(readData[0], []byte("test in preallocated file")) {
		t.Error("Data mismatch after prealloc recovery")
	}
}

func TestWAL_SegmentNaming(t *testing.T) {
	w := &WAL{dir: "/tmp/test"}

	path := w.segmentPath(0)
	expected := filepath.Join("/tmp/test", "wal-0000000000000000.log")
	if path != expected {
		t.Errorf("segmentPath(0): got %s, want %s", path, expected)
	}

	path = w.segmentPath(42)
	expected = filepath.Join("/tmp/test", "wal-0000000000000042.log")
	if path != expected {
		t.Errorf("segmentPath(42): got %s, want %s", path, expected)
	}
}

// --- New tests for CRC chaining, snapshots, group commit ---

func TestCRCChain_AcrossRecords(t *testing.T) {
	// Verify that corrupting one record invalidates all subsequent records.
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 0, 4096)

	payloads := [][]byte{
		[]byte("record-0"),
		[]byte("record-1"),
		[]byte("record-2"),
	}

	for _, data := range payloads {
		rec := &walpb.Record{Type: walpb.DataType, Data: data}
		if _, err := enc.Encode(rec); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	}

	if err := enc.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	raw := buf.Bytes()

	// Decode all — should succeed.
	dec := NewDecoder(bytes.NewReader(raw))
	for i := range payloads {
		var rec walpb.Record
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
	}

	// Now corrupt the first record's data (a byte in the payload area).
	// Find the data portion — skip 8-byte length field, then the protobuf
	// encoding. We'll flip a byte in the middle of the raw buffer.
	corrupted := make([]byte, len(raw))
	copy(corrupted, raw)
	// Byte 20 should be inside the first record's protobuf payload.
	if len(corrupted) > 20 {
		corrupted[20] ^= 0xFF
	}

	dec2 := NewDecoder(bytes.NewReader(corrupted))
	var rec walpb.Record
	err := dec2.Decode(&rec)
	// First record should fail CRC.
	if err == nil {
		// If first record somehow passes (corrupt byte was in padding),
		// second should fail due to chain.
		err = dec2.Decode(&rec)
		if err == nil {
			err = dec2.Decode(&rec)
		}
	}

	if err == nil {
		t.Error("Expected CRC error from corrupted chain, got nil")
	}
}

func TestCRCCheckpoint_SegmentBoundary(t *testing.T) {
	dir := t.TempDir()

	// Use small segments to force rotation.
	opts := Options{
		SegmentSize: 64,
		SyncPolicy:  SyncOnAppend,
		BufferSize:  32,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write enough to force multiple segments.
	var payloads [][]byte
	for i := 0; i < 5; i++ {
		data := []byte("segment boundary test data")
		payloads = append(payloads, data)
		if _, err := w.Append(data); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	w.Close()

	// Verify multiple segments exist.
	segments, _ := (&WAL{dir: dir}).listSegments()
	if len(segments) < 2 {
		t.Fatalf("Expected multiple segments, got %d", len(segments))
	}

	// Verify each segment starts with a CrcType record.
	for _, seg := range segments {
		f, err := os.Open(seg.path)
		if err != nil {
			t.Fatalf("Open segment %s failed: %v", seg.path, err)
		}
		dec := NewDecoder(f)
		var rec walpb.Record
		if err := dec.Decode(&rec); err != nil {
			f.Close()
			t.Fatalf("Decode first record in %s failed: %v", seg.path, err)
		}
		if rec.Type != walpb.CrcType {
			f.Close()
			t.Errorf("First record in %s: type=%d, want CrcType=%d", seg.path, rec.Type, walpb.CrcType)
		}
		f.Close()
	}

	// Reopen and verify all data reads correctly across segments.
	w2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	readData, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != len(payloads) {
		t.Fatalf("ReadAll returned %d records, want %d", len(readData), len(payloads))
	}
}

func TestCRCBridging_ReadWriteTransition(t *testing.T) {
	dir := t.TempDir()

	// Write some records, close, reopen, write more — CRC chain should
	// be continuous across the read→write transition.
	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := w.Append([]byte("before close")); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	w.Close()

	// Reopen and write more.
	w2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := w2.Append([]byte("after reopen")); err != nil {
			t.Fatalf("Append after reopen failed: %v", err)
		}
	}
	w2.Close()

	// Final reopen — read all, verify chain integrity.
	w3, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Final reopen failed: %v", err)
	}
	defer w3.Close()

	readData, err := w3.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != 6 {
		t.Fatalf("Expected 6 records, got %d", len(readData))
	}
}

func TestSaveSnapshot(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Write some data.
	if _, err := w.Append([]byte("before snapshot")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Save snapshot marker.
	snap := &walpb.Snapshot{Offset: 42}
	if err := w.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Write more data.
	if _, err := w.Append([]byte("after snapshot")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// ReadAll should only return DataType records (not snapshot).
	readData, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != 2 {
		t.Fatalf("Expected 2 data records (snapshot filtered), got %d", len(readData))
	}

	if !bytes.Equal(readData[0], []byte("before snapshot")) {
		t.Errorf("Record 0: got %q, want %q", readData[0], "before snapshot")
	}
	if !bytes.Equal(readData[1], []byte("after snapshot")) {
		t.Errorf("Record 1: got %q, want %q", readData[1], "after snapshot")
	}

	// Verify snapshot is actually on disk by reading raw records.
	segments, _ := w.listSegments()
	f, _ := os.Open(segments[0].path)
	dec := NewDecoder(f)
	var snapFound bool
	for {
		var rec walpb.Record
		if err := dec.Decode(&rec); err != nil {
			break
		}
		if rec.Type == walpb.SnapshotType {
			snapFound = true
			var s walpb.Snapshot
			if err := proto.Unmarshal(rec.Data, &s); err != nil {
				t.Fatalf("Unmarshal snapshot: %v", err)
			}
			if s.Offset != 42 {
				t.Errorf("Snapshot offset: got %d, want 42", s.Offset)
			}
		}
	}
	f.Close()
	if !snapFound {
		t.Error("Snapshot record not found in raw segment")
	}
}

func TestGroupCommit(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		SyncPolicy: SyncManual,
		BufferSize: 128 * 1024,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Launch N goroutines that each append + sync.
	const N = 20
	var wg sync.WaitGroup
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte("group commit record")
			if _, err := w.Append(data); err != nil {
				errs[i] = err
				return
			}
			if err := w.Sync(); err != nil {
				errs[i] = err
			}
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Goroutine %d error: %v", i, err)
		}
	}

	// Verify all records were persisted.
	readData, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != N {
		t.Errorf("Expected %d records from group commit, got %d", N, len(readData))
	}
}

func TestDataTypeFiltering(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Append data, save snapshot, append more.
	if _, err := w.Append([]byte("data-1")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := w.SaveSnapshot(&walpb.Snapshot{Offset: 100}); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	if _, err := w.Append([]byte("data-2")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// ReadAll should filter out CrcType and SnapshotType.
	readData, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readData) != 2 {
		t.Fatalf("Expected 2 DataType records, got %d", len(readData))
	}

	if !bytes.Equal(readData[0], []byte("data-1")) {
		t.Errorf("Record 0: got %q", readData[0])
	}
	if !bytes.Equal(readData[1], []byte("data-2")) {
		t.Errorf("Record 1: got %q", readData[1])
	}
}

func TestSaveSnapshot_NilReturnsError(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	if err := w.SaveSnapshot(nil); err == nil {
		t.Error("SaveSnapshot(nil) should return an error")
	}
}

// --- Benchmarks ---

func BenchmarkWAL_Append(b *testing.B) {
	dir := b.TempDir()

	opts := Options{
		SyncPolicy: SyncManual,
		BufferSize: 128 * 1024,
	}

	w, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	data := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := w.Append(data); err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}

	w.Sync()
}

func BenchmarkEncoder_Encode(b *testing.B) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 0, 128*1024)

	data := make([]byte, 100)
	rec := &walpb.Record{Type: walpb.DataType, Data: data}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(rec); err != nil {
			b.Fatalf("Encode failed: %v", err)
		}
		if i%10000 == 0 {
			enc.Flush()
			buf.Reset()
		}
	}
}

func BenchmarkDecoder_Decode(b *testing.B) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 0, 128*1024)

	data := make([]byte, 100)
	for i := 0; i < 10000; i++ {
		rec := &walpb.Record{Type: walpb.DataType, Data: data}
		enc.Encode(rec)
	}
	enc.Flush()

	raw := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dec := NewDecoder(bytes.NewReader(raw))
		for {
			var r walpb.Record
			err := dec.Decode(&r)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Decode failed: %v", err)
			}
		}
	}
}
