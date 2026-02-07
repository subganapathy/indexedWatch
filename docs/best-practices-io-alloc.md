# Best Practices: Disk I/O and Memory Allocation Optimization

Patterns extracted from the WAL encoder + PageWriter implementation, based on etcd's production design.

## Pattern 1: Pre-allocated Reusable Buffers (Zero-Alloc Hot Path)

Pre-allocate buffers at initialization. Re-slice to zero length before each use.

```go
type Encoder struct {
    buf       []byte // 1MB — allocated once in NewEncoder
    uint64buf []byte // 8B  — reused every Encode call
    padBuf    []byte // 8B  — reused every Encode call
}

func (e *Encoder) Encode(rec *Record) {
    data := e.buf[:0]                          // zero length, full capacity
    data, _ = proto.MarshalAppend(data, rec)   // appends into existing capacity
}
```

The `buf[:0]` trick creates a zero-length slice backed by the existing array. `MarshalAppend` writes into that capacity without allocating. As long as the serialized output fits (1MB covers virtually all records), zero heap allocations.

**When to use:** Any hot-path serialization — protobuf, JSON, binary encoding. Any repeatedly-written small fixed-size values (length fields, checksums, padding).

**Key API:** `proto.MarshalOptions{}.MarshalAppend(buf[:0], msg)` — the standard protobuf zero-alloc pattern.

## Pattern 2: Buffered Writes with Page-Aligned Flushing

Absorb many small `Write()` calls into a large buffer, flush as a single syscall.

```go
func (pw *PageWriter) Write(p []byte) (n int, err error) {
    if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
        copy(pw.buf[pw.bufferedBytes:], p)  // memcpy, no syscall
        pw.bufferedBytes += len(p)
        return len(p), nil
    }
    // overflow path: align, flush, direct-write pages, buffer tail
}
```

A typical 100-byte record produces ~120 bytes of framed data (length field + protobuf + padding). At 128KB buffer, that's ~1,000 records buffered before one `write(2)` syscall. Without buffering, every Encode would be 2-3 `write(2)` syscalls.

**Why it matters:** The cost of one 128KB `write(2)` is almost the same as one 120-byte `write(2)` — syscall overhead dominates at small sizes. Buffering turns 3,000 syscalls into 1.

**When to use:** Any write-heavy path where individual writes are small relative to the syscall overhead (~1-5μs per syscall on Linux).

## Pattern 3: Page Alignment for Disk I/O Efficiency

Flush on 4KB page boundaries, not arbitrary sizes.

```
Buffer: [============================|====]
        ^--- 128KB watermark ---^    ^--- 4KB slack page
```

Why page alignment matters:
1. **OS page cache is 4KB.** An unaligned write can dirty two pages instead of one.
2. **Disk sectors are 512B.** 4KB alignment (8 sectors) matches filesystem block sizes.
3. **Direct I/O requires alignment.** If switching to `O_DIRECT`, this is mandatory.
4. **Torn write detection.** 8-byte frame alignment within 512-byte sectors means length fields never span sectors.

The slack mechanism fills up to the next page boundary before flushing, ensuring aligned I/O.

For large writes exceeding the buffer, bypass it entirely:
```go
if len(p) > pw.pageBytes {
    pages := len(p) / pw.pageBytes
    pw.w.Write(p[:pages*pw.pageBytes])  // direct write, no copy
}
```

**When to use:** Any buffered writer targeting file I/O. Especially important for write-ahead logs, LSM compaction output, and any `O_DIRECT` path.

## Pattern 4: Separation of Encode from Sync

The most expensive operation is `fdatasync` (~0.1-10ms). Separate the cheap per-item work (serialize + buffer) from the expensive batch operation (sync):

```
Append(rec1) → encode into buffer         ← ~60ns
Append(rec2) → encode into buffer         ← ~60ns
Append(rec3) → encode into buffer         ← ~60ns
Sync()       → flush buffer + fdatasync   ← ~1ms (amortized over 3 records)
```

The caller controls when to sync. For single-writer schemas, call `Sync()` after each `Append()`. For high-throughput resource writes, N goroutines calling `Sync()` share one fdatasync via group commit (leader-follower):

```
G1: Append → Sync() ──┐
G2: Append → Sync() ──┤── leader does 1 fdatasync, notifies all
G3: Append → Sync() ──┘
```

**When to use:** Any durable write path. The general principle: make per-item work O(1) with no syscalls, then amortize the O(sync) cost across a batch.

## Pattern 5: Overflow Buffer (Slack Space)

Allocate extra space beyond the nominal buffer size for alignment:

```go
buf: make([]byte, bufferSize + pageBytes)  // 128KB + 4KB = 132KB
bufWatermarkBytes: bufferSize              // trigger overflow at 128KB
```

The extra 4KB is slack. When the buffer is almost full and the write position is unaligned, the slack allows completing the current page without forcing a premature unaligned flush. The watermark (128KB) triggers the overflow path, but there's room to finish page alignment before the actual `write(2)`.

**When to use:** Any buffer with alignment constraints. Without slack, every near-full buffer triggers an unaligned flush, defeating the alignment optimization.

## Summary

| Pattern | I/O Saving | Alloc Saving |
|---------|-----------|-------------|
| Pre-allocated marshal buffer (`buf[:0]`) | — | Eliminates per-record alloc |
| Reusable small buffers (`uint64buf`, `padBuf`) | — | Eliminates per-write alloc |
| Write buffering (128KB PageWriter) | ~1000x fewer `write(2)` syscalls | — |
| Page-aligned flushing (4KB) | Avoids dirtying extra OS pages | — |
| Large-write bypass | Avoids copying >4KB through buffer | — |
| Encode/Sync separation | fdatasync amortized across N records | — |
| Slack page in buffer | Avoids unaligned flushes | — |

## Full Write Stack

```
Append(data)                              ← WAL: builds Record{DataType, data}
  └─ encoder.Encode(rec)                  ← Encoder: CRC chain + marshal + frame
       ├─ crc.Write(rec.Data)             ← hash update into running chain, no alloc
       ├─ MarshalAppend(buf[:0], rec)     ← protobuf into pre-alloc'd 1MB buf
       ├─ pw.Write(uint64buf)             ← 8-byte length field
       ├─ pw.Write(data)                  ← marshaled protobuf bytes
       └─ pw.Write(padBuf[:n])            ← 0-7 padding bytes for 8-byte alignment
                                           ↓
                              PageWriter: copy into 128KB buffer
                              No syscall unless buffer overflows
                                           ↓
                              Flush() → single write(2) syscall
                              Sync()  → single fdatasync syscall
```

Benchmark result: **0 allocs/op** on the Encode hot path, **~60ns/op** per record.
