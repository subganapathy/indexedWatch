package wal

// SyncPolicy controls when fsync happens.
type SyncPolicy int

const (
	// SyncOnAppend fsyncs after each Append (safe, slow).
	// Use for schemas where writes are rare but durability is critical.
	SyncOnAppend SyncPolicy = iota

	// SyncManual requires the caller to call Sync() explicitly.
	// Use for resources where writes are batched.
	SyncManual
)

// Options configures WAL behavior.
type Options struct {
	// SegmentSize is the maximum size per WAL file in bytes.
	// When the current segment exceeds this size, a new segment is created.
	// Set to 0 to disable rotation (single file mode).
	// Default: 0 (no rotation)
	SegmentSize int64

	// SyncPolicy controls when fsync happens.
	// Default: SyncOnAppend
	SyncPolicy SyncPolicy

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
		SegmentSize:  0,                   // No rotation
		SyncPolicy:   SyncOnAppend,        // Safe by default
		BufferSize:   128 * 1024,          // 128KB
		PreallocSize: 0,                   // No preallocation
	}
}

// SchemaOptions returns options optimized for schema WAL.
// - No rotation (schemas are small)
// - Sync on every append (durability critical)
// - Small buffer (writes are rare)
func SchemaOptions() Options {
	return Options{
		SegmentSize:  0,            // No rotation
		SyncPolicy:   SyncOnAppend, // Immediate durability
		BufferSize:   4 * 1024,     // 4KB buffer (schemas are rare)
		PreallocSize: 0,            // No preallocation
	}
}

// ResourceOptions returns options optimized for resource WAL.
// - 64MB segments with rotation
// - Manual sync (caller batches)
// - Large buffer for throughput
// - Preallocation for performance
func ResourceOptions() Options {
	return Options{
		SegmentSize:  64 * 1024 * 1024, // 64MB segments
		SyncPolicy:   SyncManual,       // Caller controls sync
		BufferSize:   128 * 1024,       // 128KB buffer
		PreallocSize: 64 * 1024 * 1024, // Preallocate segments
	}
}
