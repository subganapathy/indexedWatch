package wal

// Options configures WAL behavior.
type Options struct {
	// SegmentSize is the rotation threshold per WAL segment in bytes.
	// When the current segment's offset reaches this threshold, a new
	// segment is created on the next Append. Individual records may cause
	// the segment to slightly exceed this size.
	// Set to 0 to disable rotation (single file mode).
	// Default: 0 (no rotation)
	SegmentSize int64

	// BufferSize is the write buffer size in bytes.
	// Larger buffers reduce syscalls but increase memory usage.
	// Default: 128KB
	BufferSize int

	// PreallocSize is the file preallocation size in bytes.
	// Preallocation can improve write performance by reducing fragmentation.
	// Set to 0 to disable preallocation.
	// Default: 0
	PreallocSize int64
}

// DefaultOptions returns the default WAL options.
func DefaultOptions() Options {
	return Options{
		SegmentSize:  0,          // No rotation
		BufferSize:   128 * 1024, // 128KB
		PreallocSize: 0,          // No preallocation
	}
}

// SchemaOptions returns options optimized for schema WAL.
// - No rotation (schemas are small)
// - Small buffer (writes are rare)
// Caller should call Sync() after each Append for immediate durability.
func SchemaOptions() Options {
	return Options{
		SegmentSize:  0,        // No rotation
		BufferSize:   4 * 1024, // 4KB buffer (schemas are rare)
		PreallocSize: 0,        // No preallocation
	}
}

// ResourceOptions returns options optimized for resource WAL.
// - 64MB segments with rotation
// - Large buffer for throughput
// - Preallocation for performance and torn write detection
// Caller batches Appends and calls Sync() to amortize fdatasync cost.
// Concurrent Sync() callers share a single fdatasync via group commit.
func ResourceOptions() Options {
	return Options{
		SegmentSize:  64 * 1024 * 1024, // 64MB segments
		BufferSize:   128 * 1024,       // 128KB buffer
		PreallocSize: 64 * 1024 * 1024, // Preallocate segments
	}
}
