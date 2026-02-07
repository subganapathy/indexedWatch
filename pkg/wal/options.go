package wal

// SyncPolicy controls when Sync (fdatasync) happens relative to Append.
// Both policies use the same group commit mechanism under the hood:
// concurrent Sync() callers share a single fdatasync via leader-follower.
type SyncPolicy int

const (
	// SyncOnAppend calls Sync() automatically after each Append.
	// Safe but slower — every Append pays the fdatasync cost.
	// Use for schemas where writes are rare but durability is critical.
	SyncOnAppend SyncPolicy = iota

	// SyncManual requires the caller to call Sync() explicitly after
	// one or more Appends. Multiple goroutines calling Append+Sync
	// concurrently share a single fdatasync via group commit.
	// Use for resources where multiple Puts are batched.
	SyncManual
)

// Options configures WAL behavior.
type Options struct {
	// SegmentSize is the rotation threshold per WAL segment in bytes.
	// When the current segment's offset reaches this threshold, a new
	// segment is created on the next Append. Individual records may cause
	// the segment to slightly exceed this size.
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
// - Manual sync with group commit (multiple Puts share one fdatasync)
// - Large buffer for throughput
// - Preallocation for performance and torn write detection
func ResourceOptions() Options {
	return Options{
		SegmentSize:  64 * 1024 * 1024, // 64MB segments
		SyncPolicy:   SyncManual,       // Caller controls sync
		BufferSize:   128 * 1024,       // 128KB buffer
		PreallocSize: 64 * 1024 * 1024, // Preallocate segments
	}
}
