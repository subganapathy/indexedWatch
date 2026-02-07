# etcd WAL Implementation Walkthrough

Deep code walkthrough of etcd's WAL (`server/storage/wal/`), traced with exact file:line references against the etcd source at `/Users/subramanianganapathy/code/lsm-stuff/etcd/`.

## Source Files

| File | Lines | Purpose |
|------|-------|---------|
| `server/storage/wal/walpb/record.proto` | 27 | Record + Snapshot protobuf definitions |
| `server/storage/wal/wal.go` | 1050 | Core WAL: Create, Open, Save, Cut, ReadAll, ReleaseLockTo |
| `server/storage/wal/encoder.go` | 134 | Frame encoding, CRC computation, PageWriter integration |
| `server/storage/wal/decoder.go` | 230 | Frame decoding, CRC validation, torn write detection |
| `server/storage/wal/util.go` | 113 | File naming, searchIndex, isValidSeq |
| `server/storage/wal/file_pipeline.go` | 107 | Async segment preallocation |
| `server/storage/wal/repair.go` | 117 | Last-file truncation repair |
| `server/storage/wal/metrics.go` | 53 | Prometheus metrics |
| `pkg/ioutil/pagewriter.go` | 116 | Page-aligned buffered writer |
| `pkg/crc/crc.go` | 54 | Seedable CRC-32 digest |
| `contrib/raftexample/raft.go` | 527 | Complete Raft+WAL integration example |

---

## Layer 1: Data Model — What Gets Stored

### The Record Envelope

`walpb/record.proto:14-18`:
```protobuf
message Record {
    optional int64 type  = 1 [(gogoproto.nullable) = false];
    optional uint32 crc  = 2 [(gogoproto.nullable) = false];
    optional bytes data  = 3;
}
```

Three fields. Every single thing the WAL persists is wrapped in this envelope. `data` is opaque — the WAL never interprets it. `type` tells the reader how to deserialize. The `gogoproto` annotations generate `MarshalTo(buf)` for zero-alloc marshaling.

### The Five Record Types

`wal.go:38-43`:
```go
const (
    MetadataType int64 = iota + 1  // 1
    EntryType                       // 2
    StateType                       // 3
    CrcType                         // 4
    SnapshotType                    // 5
)
```

`iota + 1` starts at 1 — type 0 would be ambiguous with uninitialized Record.

**Order in a freshly created WAL** (wal.go:166-173):
1. CrcType — CRC chain checkpoint (prevCrc=0)
2. MetadataType — cluster metadata
3. SnapshotType — empty snapshot marker

**During normal operation** (wal.go:967-973):
4. EntryType — each Raft entry, marshaled then wrapped
5. StateType — Raft HardState (term, vote, commit), always after all entries

### The Snapshot Marker

`walpb/record.proto:21-26`:
```protobuf
message Snapshot {
    optional uint64 index = 1;
    optional uint64 term  = 2;
    optional raftpb.ConfState conf_state = 3;
}
```

Metadata only — NOT snapshot data. The actual snapshot lives in a separate directory. Snapshot file written first, WAL marker second (raftexample/raft.go:125-127). If crash between, orphaned file (harmless) rather than dangling pointer (fatal).

### Key Insight
WAL record types are WAL-level concerns (CRC checkpoint, metadata, snapshot marker), not application concerns. Schema vs resource distinction belongs above the WAL.

---

## Layer 2: Wire Format — How It's Laid Out on Disk

### The Frame
```
+------------------+---------------------+------------+
| Length Field (8B) | Protobuf Data (var) | Pad (0-7B) |
+------------------+---------------------+------------+
<-------------- always 8-byte aligned total ----------->
```

### Length Field Encoding

`encoder.go:93-101`:
```go
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
    lenField = uint64(dataBytes)
    padBytes = (8 - (dataBytes % 8)) % 8
    if padBytes != 0 {
        lenField |= uint64(0x80|padBytes) << 56
    }
    return lenField, padBytes
}
```

- Bits 0-55: payload byte length (56 bits, max ~72 PB)
- Bits 56-63: `0x80 | padBytes` if padding needed, else `0x00`
- Setting bit 63 makes signed int64 negative — decoder uses `lenField < 0` as fast check

### Length Field Decoding

`decoder.go:155-163`:
```go
func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
    recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
    if lenField < 0 {
        padBytes = int64((uint64(lenField) >> 56) & 0x7)
    }
    return recBytes, padBytes
}
```

### Why 8-Byte Alignment

The 8-byte length field always starts at an 8-byte-aligned offset. Since 8 divides into 512-byte sectors, the length field always falls within one sector. Sectors are atomic — a torn write either gives the full correct 8 bytes or zeros (from preallocation). Zero means "stop reading." This is the foundation of torn write safety.

---

## Layer 3: CRC Chain — Integrity Guarantees

### CRC-32 Castagnoli

`wal.go:64`: `crcTable = crc32.MakeTable(crc32.Castagnoli)`

Better error detection than IEEE CRC-32. Hardware-accelerated via SSE4.2 on x86.

### Seedable CRC Digest

`pkg/crc/crc.go:27-48`:
```go
func New(prev uint32, tab *crc32.Table) hash.Hash32 { return &digest{prev, tab} }

func (d *digest) Write(p []byte) (n int, err error) {
    d.crc = crc32.Update(d.crc, d.tab, p)
    return len(p), nil
}
```

`New(prev, tab)` seeds with a previous CRC value. This enables chaining across records and segment files.

### How the Chain Works

`encoder.go:63-68`:
```go
e.crc.Write(rec.Data)       // feed THIS record's data into running hash
rec.Crc = e.crc.Sum32()     // store the CUMULATIVE hash
```

Record N's CRC = CRC(data_1 + data_2 + ... + data_N). If any prior record is corrupted/reordered/deleted, all subsequent CRCs fail. Per-record CRC only detects corruption within a single record. Chaining detects corruption of the sequence.

### CRC Checkpoints at Segment Boundaries

`wal.go:1014-1016`:
```go
func (w *WAL) saveCrc(prevCrc uint32) error {
    return w.encoder.encode(&walpb.Record{Type: CrcType, Crc: prevCrc})
}
```

During segment cut (wal.go:770-776): `prevCrc = encoder.crc.Sum32()` → seed new encoder → write CrcType record. During decode (wal.go:511-519): validate chain, then `decoder.UpdateCRC(rec.Crc)` to reset chain from checkpoint.

### ReadAll → Write Mode Bridging

`wal.go:581-583`:
```go
w.encoder, err = newFileEncoder(w.tail().File, w.decoder.LastCRC())
```

Decoder's final CRC seeds the encoder. Chain is continuous across read-to-write transition.

---

## Layer 4: Encoder — Writing Records

### Structure

`encoder.go:35-51`:
```go
type encoder struct {
    mu        sync.Mutex
    bw        *ioutil.PageWriter    // 4KB-page buffered writer
    crc       hash.Hash32           // running CRC chain
    buf       []byte                // 1MB pre-allocated marshal buffer
    uint64buf []byte                // 8-byte reusable length field buffer
}
```

### The Hot Path

`encoder.go:63-91`:
1. Lock (line 64)
2. CRC update (line 67): `e.crc.Write(rec.Data)` — chain BEFORE marshaling
3. Store CRC (line 68): `rec.Crc = e.crc.Sum32()` — set before marshal since CRC is in the output
4. Marshal (lines 75-86): `rec.MarshalTo(e.buf)` for < 1MB (zero-alloc), `rec.Marshal()` for > 1MB (rare)
5. Pad (line 88): `prepareDataWithPadding(data)`
6. Write frame (line 90): `write(e.bw, e.uint64buf, data, lenField)` — 2 writes to PageWriter

### PageWriter — Buffered Page-Aligned I/O

`pkg/ioutil/pagewriter.go`:
- 128KB buffer + 4KB slack page (line 51)
- Fast path (line 57-61): fits in buffer → `copy`, no syscall
- Slow path: complete partial page → flush → direct-write full pages → buffer tail
- `pageOffset` parameter from `newFileEncoder` ensures alignment when reopening mid-file

### The Complete Write Stack

```
Save(hardState, entries)
  ├─ saveEntry (×N) → encoder.encode → PageWriter buffer  [no syscall]
  ├─ saveState      → encoder.encode → PageWriter buffer  [no syscall]
  └─ if mustSync:
       sync() → flush PageWriter → fdatasync             [2 syscalls]
```

Multiple records batched into minimal I/O. `fdatasync` (not `fsync`) skips metadata updates.

### Why etcd Syncs Per-Save (Not Group Commit)

Raft protocol requires: persist → Advance → next Ready. Can't batch across Ready boundaries without delaying acknowledgment. But batching happens within a Ready batch (multiple entries, one fdatasync). For non-Raft systems, group commit across concurrent writers is the right optimization.

---

## Layer 5: Decoder — Reading Records

### Structure

`decoder.go:44-56`:
```go
type decoder struct {
    mu                 sync.Mutex
    brs                []*fileutil.FileBufReader  // one per segment file
    lastValidOff       int64                      // truncation-safe offset
    crc                hash.Hash32                // running CRC chain
    continueOnCrcError bool                       // for inspection tools
}
```

### The decodeRecord Hot Path

`decoder.go:86-153`:
1. Read 8-byte length field (line 92)
2. EOF/zero → advance to next segment: `d.brs = d.brs[1:]`, recurse (lines 93-101)
3. Decode frame size (line 106)
4. Bounds check: recBytes must not exceed remaining file size (lines 108-112) — OOM protection
5. Read data+padding via `io.ReadFull` (lines 114-122) — allocates per record (acceptable at startup)
6. Unmarshal protobuf (lines 123-128) — if fails, check torn write
7. CRC validate: update running CRC, compare with stored (lines 130-148)
8. Advance `lastValidOff` (line 151)

### Torn Write Detection

`decoder.go:168-201`:
- Only checks last segment file (`len(d.brs) != 1` → false)
- Splits record data on 512-byte sector boundaries
- If any sector chunk is all zeros → torn write
- Relies on preallocation (zeros) and atomic sector writes
- Returns `io.ErrUnexpectedEOF` (recoverable) not hard corruption

### Error Classification

| Error | Meaning | Action |
|-------|---------|--------|
| `io.EOF` | Clean end | Normal |
| `io.ErrUnexpectedEOF` | Torn write | Truncate at `LastOffset()` |
| `ErrCRCMismatch` | Data corruption | Operator intervention |

### Post-Decode Cleanup (wal.go:558-564)

In write mode, after EOF: seek to `LastOffset()`, `ZeroToEnd()`. Zeros out partial writes to prevent future CRC errors.

---

## Layer 6: WAL Lifecycle (Layers 6-7 not yet covered in walkthrough)

### Create (wal.go:100-234)
Atomic: temp dir → write initial records → rename → fsync parent dir

### Open (wal.go:344-397)
Select files by snapshot index → lock → create decoder → start FilePipeline

### Save (wal.go:955-991)
Encode entries + state → conditional sync (MustSync) → cut if > 64MB

### Cut (wal.go:745-827)
Truncate old → sync → get pre-allocated file from pipeline → bridge CRC → write header → atomic rename → fsync dir

### ReadAll (wal.go:469-591)
Decode all records → classify errors → zero trailing garbage → transition read→write mode

### FilePipeline (file_pipeline.go:27-106)
Background goroutine pre-creates 64MB files. Alternates `0.tmp`/`1.tmp`. Channel-based: `cut()` gets files instantly.

### Repair (repair.go:32-106)
Opens last segment → decode until error → `ErrUnexpectedEOF` → backup + truncate + fsync

---

## Layer 7: Raft Integration

### Ready Loop (raftexample/raft.go:455-474)
```
Ready → saveSnap (if any) → wal.Save → applySnapshot → append → send → apply → maybeSnapshot → Advance
```
WAL save MUST happen before in-memory state changes or network messages.

### Snapshot Coordination (raftexample/raft.go:119-135)
1. Save snapshot file first (orphaned = harmless)
2. Write WAL marker second (dangling pointer = fatal)
3. ReleaseLockTo (free old segment locks)

### Recovery (raftexample/raft.go:247-265)
1. loadSnapshot (cross-reference WAL markers with disk files)
2. openWAL(snapshot)
3. ReadAll → metadata, state, entries
4. ApplySnapshot + SetHardState + Append → Raft ready

---

## Key Differences: etcd WAL vs indexedWatch WAL

| Aspect | etcd | indexedWatch (current) |
|--------|------|----------------------|
| Record format | Protobuf (gogoproto MarshalTo) | Hand-rolled binary (16B header) |
| CRC model | **Chained** (cumulative) | **Per-record** (independent) |
| Record types | 5 WAL-level types | Generic (application-defined) |
| Segment naming | `%016x-%016x.wal` (seq + index) | `wal-%016d.log` (seq only) |
| File locking | `fileutil.LockedFile` (flock) | None |
| Preallocation | FilePipeline (async goroutine) | Synchronous `Truncate` |
| WAL modes | Read → ReadAll → Write | Always appendable |
| Sync policy | MustSync from Raft | SyncOnAppend or SyncManual |
| Snapshot markers | Yes (metadata-only) | No |
| Atomic creation | Temp dir → rename → fsync parent | Direct creation |
| CRC bridging | CrcType records bridge across segments | No chain |
| Metrics | Prometheus histograms | None |
