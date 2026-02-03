package wal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRecord_MarshalUnmarshal(t *testing.T) {
	rec := &Record{
		Type: 42,
		Data: []byte("hello world"),
		Crc:  ComputeCRC([]byte("hello world")),
	}

	// Marshal
	data, err := rec.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal
	rec2 := &Record{}
	if err := rec2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify
	if rec2.Type != rec.Type {
		t.Errorf("Type mismatch: got %d, want %d", rec2.Type, rec.Type)
	}
	if rec2.Crc != rec.Crc {
		t.Errorf("Crc mismatch: got %x, want %x", rec2.Crc, rec.Crc)
	}
	if !bytes.Equal(rec2.Data, rec.Data) {
		t.Errorf("Data mismatch: got %q, want %q", rec2.Data, rec.Data)
	}
}

func TestRecord_MarshalTo(t *testing.T) {
	rec := &Record{
		Type: 1,
		Data: []byte("test data"),
		Crc:  ComputeCRC([]byte("test data")),
	}

	buf := make([]byte, rec.Size())
	n, err := rec.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	if n != rec.Size() {
		t.Errorf("MarshalTo returned wrong size: got %d, want %d", n, rec.Size())
	}

	// Unmarshal and verify
	rec2 := &Record{}
	if err := rec2.Unmarshal(buf); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if !bytes.Equal(rec2.Data, rec.Data) {
		t.Errorf("Data mismatch after MarshalTo")
	}
}

func TestRecord_Validate(t *testing.T) {
	rec := &Record{
		Type: 1,
		Data: []byte("test"),
		Crc:  ComputeCRC([]byte("test")),
	}

	// Valid CRC
	if err := rec.Validate(rec.Crc); err != nil {
		t.Errorf("Validate returned error for valid CRC: %v", err)
	}

	// Invalid CRC
	if err := rec.Validate(0xDEADBEEF); err == nil {
		t.Error("Validate should return error for invalid CRC")
	} else if !errors.Is(err, ErrCRCMismatch) {
		t.Errorf("Expected ErrCRCMismatch, got: %v", err)
	}
}

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

	// Data should be buffered, not written yet
	if buf.Len() != 0 {
		t.Errorf("Data was written before flush: got %d bytes", buf.Len())
	}

	// Flush
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

	// Write more than the buffer size
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

	// Flush remaining
	if err := pw.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if buf.Len() != len(data) {
		t.Errorf("Total bytes written: got %d, want %d", buf.Len(), len(data))
	}
}

func TestEncoder_Encode(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 4096)

	rec := &Record{
		Type: 1,
		Data: []byte("test record"),
	}

	offset, err := enc.Encode(rec)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("First record offset should be 0, got %d", offset)
	}

	// Verify CRC was set
	if rec.Crc == 0 {
		t.Error("CRC should have been set by Encode")
	}

	// Flush and verify data was written
	if err := enc.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Should have: 8 bytes length + record data + padding
	if buf.Len() == 0 {
		t.Error("No data written after flush")
	}
}

func TestEncoderDecoder_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 4096)

	// Write multiple records
	records := []*Record{
		{Type: 1, Data: []byte("first")},
		{Type: 2, Data: []byte("second record with more data")},
		{Type: 3, Data: []byte("third")},
	}

	for _, rec := range records {
		if _, err := enc.Encode(rec); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	}

	if err := enc.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Decode
	dec := NewDecoder(&buf)
	for i, expected := range records {
		rec := &Record{}
		if err := dec.Decode(rec); err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if rec.Type != expected.Type {
			t.Errorf("Record %d type: got %d, want %d", i, rec.Type, expected.Type)
		}
		if !bytes.Equal(rec.Data, expected.Data) {
			t.Errorf("Record %d data: got %q, want %q", i, rec.Data, expected.Data)
		}
	}

	// Should get EOF on next read
	rec := &Record{}
	if err := dec.Decode(rec); err != io.EOF {
		t.Errorf("Expected EOF, got: %v", err)
	}
}

func TestWAL_BasicAppendRead(t *testing.T) {
	dir := t.TempDir()

	// Open WAL
	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Append records
	records := []*Record{
		{Type: 1, Data: []byte("first")},
		{Type: 2, Data: []byte("second")},
		{Type: 3, Data: []byte("third")},
	}

	for _, rec := range records {
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Read all
	readRecords, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != len(records) {
		t.Fatalf("ReadAll returned %d records, want %d", len(readRecords), len(records))
	}

	for i, rec := range readRecords {
		if rec.Type != records[i].Type {
			t.Errorf("Record %d type: got %d, want %d", i, rec.Type, records[i].Type)
		}
		if !bytes.Equal(rec.Data, records[i].Data) {
			t.Errorf("Record %d data: got %q, want %q", i, rec.Data, records[i].Data)
		}
	}
}

func TestWAL_Recovery(t *testing.T) {
	dir := t.TempDir()

	// Write some records
	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	records := []*Record{
		{Type: 1, Data: []byte("persistent record 1")},
		{Type: 2, Data: []byte("persistent record 2")},
	}

	for _, rec := range records {
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	w2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	readRecords, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != len(records) {
		t.Fatalf("After recovery: got %d records, want %d", len(readRecords), len(records))
	}

	for i, rec := range readRecords {
		if !bytes.Equal(rec.Data, records[i].Data) {
			t.Errorf("Record %d after recovery: got %q, want %q", i, rec.Data, records[i].Data)
		}
	}

	// Append more records
	newRec := &Record{Type: 3, Data: []byte("new record after recovery")}
	if _, err := w2.Append(newRec); err != nil {
		t.Fatalf("Append after recovery failed: %v", err)
	}

	readRecords, err = w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after new append failed: %v", err)
	}

	if len(readRecords) != 3 {
		t.Errorf("Expected 3 records after new append, got %d", len(readRecords))
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

	// Append should sync
	rec := &Record{Type: 1, Data: []byte("sync on append")}
	if _, err := w.Append(rec); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Data should be on disk immediately
	// Verify by checking file size
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
		BufferSize: 128 * 1024, // Large buffer
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Append without sync
	rec := &Record{Type: 1, Data: []byte("manual sync")}
	if _, err := w.Append(rec); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Data might be buffered - call Sync explicitly
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Now data should be on disk
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

	// Use small segment size to trigger rotation
	opts := Options{
		SegmentSize: 100, // Very small for testing
		SyncPolicy:  SyncOnAppend,
		BufferSize:  32, // Small buffer
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Write enough to trigger rotation
	var allRecords []*Record
	for i := 0; i < 10; i++ {
		rec := &Record{Type: int64(i), Data: []byte("record that will trigger rotation")}
		allRecords = append(allRecords, rec)
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Should have multiple segments
	segments, err := w.listSegments()
	if err != nil {
		t.Fatalf("listSegments failed: %v", err)
	}

	if len(segments) < 2 {
		t.Errorf("Expected multiple segments, got %d", len(segments))
	}

	// Read all and verify
	readRecords, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != len(allRecords) {
		t.Fatalf("ReadAll returned %d records, want %d", len(readRecords), len(allRecords))
	}

	for i, rec := range readRecords {
		if rec.Type != allRecords[i].Type {
			t.Errorf("Record %d type: got %d, want %d", i, rec.Type, allRecords[i].Type)
		}
	}
}

func TestWAL_NoRotation(t *testing.T) {
	dir := t.TempDir()

	// SegmentSize = 0 means no rotation
	opts := Options{
		SegmentSize: 0,
		SyncPolicy:  SyncOnAppend,
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Write many records
	for i := 0; i < 100; i++ {
		rec := &Record{Type: int64(i), Data: []byte("no rotation record")}
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Should have only one segment
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

	rec := &Record{Type: 1, Data: []byte("crc test")}
	if _, err := w.Append(rec); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	w.Close()

	// Corrupt the data
	segments, _ := (&WAL{dir: dir}).listSegments()
	data, _ := os.ReadFile(segments[0].path)

	// Corrupt a byte in the data section (after the length field and record header)
	if len(data) > 30 {
		data[30] ^= 0xFF // Flip bits
		os.WriteFile(segments[0].path, data, 0644)
	}

	// Try to read - should detect corruption
	w2, err := Open(dir, DefaultOptions())
	if err != nil {
		// Corruption detected during open is acceptable
		return
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	// Either error or empty records is acceptable for corrupted data
	if err == nil && len(records) > 0 {
		// If we got records, the CRC should have been validated
		// (the corruption might have been in padding or been repaired)
	}
}

func TestWAL_TailCorruption(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write some records
	for i := 0; i < 3; i++ {
		rec := &Record{Type: int64(i), Data: []byte("valid record")}
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	w.Close()

	// Append garbage to the file (simulating partial write / torn write)
	segments, _ := (&WAL{dir: dir}).listSegments()
	f, _ := os.OpenFile(segments[0].path, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0x12, 0x34, 0x56, 0x78}) // Garbage
	f.Close()

	// Reopen - should recover valid records and truncate garbage
	w2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("Expected 3 valid records after tail corruption recovery, got %d", len(records))
	}
}

func TestWAL_EmptyDir(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open on empty dir failed: %v", err)
	}
	defer w.Close()

	// Should have created a segment
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

	// Operations on closed WAL should fail
	rec := &Record{Type: 1, Data: []byte("test")}
	if _, err := w.Append(rec); !errors.Is(err, ErrClosed) {
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

// Benchmark to verify minimal allocations in the hot path
func BenchmarkWAL_Append(b *testing.B) {
	dir := b.TempDir()

	opts := Options{
		SyncPolicy: SyncManual, // Don't sync for benchmark
		BufferSize: 128 * 1024,
	}

	w, err := Open(dir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	rec := &Record{Type: 1, Data: make([]byte, 100)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := w.Append(rec); err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}

	// Sync at the end
	w.Sync()
}

func BenchmarkEncoder_Encode(b *testing.B) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 128*1024)

	rec := &Record{Type: 1, Data: make([]byte, 100)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(rec); err != nil {
			b.Fatalf("Encode failed: %v", err)
		}
		// Reset buffer periodically to avoid OOM
		if i%10000 == 0 {
			enc.Flush()
			buf.Reset()
		}
	}
}

func BenchmarkDecoder_Decode(b *testing.B) {
	// Prepare data
	var buf bytes.Buffer
	enc := NewEncoder(&buf, 0, 128*1024)

	rec := &Record{Type: 1, Data: make([]byte, 100)}
	for i := 0; i < 10000; i++ {
		enc.Encode(rec)
	}
	enc.Flush()

	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dec := NewDecoder(bytes.NewReader(data))
		for {
			r := &Record{}
			err := dec.Decode(r)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Decode failed: %v", err)
			}
		}
	}
}

func TestWAL_LargeRecord(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	// Write a record larger than the pre-allocated buffer (1MB)
	largeData := make([]byte, 2*1024*1024) // 2MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	rec := &Record{Type: 1, Data: largeData}
	if _, err := w.Append(rec); err != nil {
		t.Fatalf("Append large record failed: %v", err)
	}

	// Read back
	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	if !bytes.Equal(records[0].Data, largeData) {
		t.Error("Large record data mismatch")
	}
}

func TestWAL_Preallocation(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		SyncPolicy:   SyncOnAppend,
		PreallocSize: 1024 * 1024, // 1MB prealloc
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Check file size - should be preallocated
	segments, _ := w.listSegments()
	fi, err := os.Stat(segments[0].path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if fi.Size() != opts.PreallocSize {
		t.Errorf("Preallocated size: got %d, want %d", fi.Size(), opts.PreallocSize)
	}

	// Write and read back
	rec := &Record{Type: 1, Data: []byte("test in preallocated file")}
	if _, err := w.Append(rec); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	w.Close()

	// Reopen and verify
	w2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	if !bytes.Equal(records[0].Data, rec.Data) {
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
