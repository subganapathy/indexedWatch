# IndexedWatch - Architecture Document

## Project Vision

A storage engine providing:
- **gRPC API** for schema registration and resource CRUD
- **K8s-style Watch** semantics (snapshot + deltas, per-type ordering)
- **Multi-index lookups** (primary key, secondary keys)
- **Strong consistency** option (writer blocks until indexed)
- **Future**: Raft-based replication for read scaling

---

## Storage Layout

```
/data/
  /schemas/                         # Global schema WAL (grows forever)
    wal_index                       # Maps seqNum ranges → files
    000001.wal
    000002.wal
    materialized/                   # Schema registry (rebuilt from WAL)
      events/
        v1.json
        v2.json
        CURRENT -> v2
      users/
        v1.json
        CURRENT -> v1

  /types/
    /events/                        # Per-type storage (independent)
      wal_index                     # Maps seqNum ranges → files
      000001.wal
      000002.wal
      snapshot/                     # State Machine checkpoint
      lsms/
        user_id/                    # SK LSM
        timestamp/                  # SK LSM

    /users/                         # Another type (independent)
      wal_index
      000001.wal
      snapshot/
      lsms/
        email/
        org_id/
```

---

## Core Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              gRPC API                                        │
│  SchemaService: Register/Update schemas                                     │
│  ResourceService: Create/Update/Delete resources                            │
│  WatchService: Snapshot + Stream deltas (per-type)                          │
│  QueryService: Get by PK, List by SK                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
┌───────────────────────────────────┐ ┌───────────────────────────────────────┐
│         Schema WAL                │ │         Per-Type Storage              │
│  (Global, grows forever)          │ │  (Independent per resource type)      │
│                                   │ │                                       │
│  ┌─────────────────────────────┐  │ │  ┌─────────────────────────────────┐  │
│  │ Schema WAL                  │  │ │  │ Type: "events"                  │  │
│  │ seqNum → schema definition  │  │ │  │                                 │  │
│  └─────────────────────────────┘  │ │  │  WAL ──→ State Machine          │  │
│              │                    │ │  │   │      (pk → record)          │  │
│              ▼                    │ │  │   │                             │  │
│  ┌─────────────────────────────┐  │ │  │   └──→ SK LSMs                  │  │
│  │ Materialized Registry       │  │ │  │        (sk,pk) → seqNum        │  │
│  │ (type, version) → schema    │  │ │  └─────────────────────────────────┘  │
│  └─────────────────────────────┘  │ │                                       │
└───────────────────────────────────┘ │  ┌─────────────────────────────────┐  │
                                      │  │ Type: "users"                   │  │
                                      │  │  (same structure)               │  │
                                      │  └─────────────────────────────────┘  │
                                      └───────────────────────────────────────┘
```

### Per-Type Independence

Each resource type is fully independent (like K8s resources):
- **Separate WAL**: Own seqNum sequence
- **Separate State Machine**: Own snapshot
- **Separate LSMs**: Own secondary indexes
- **Separate pruning**: Own lifecycle

Cross-type ordering is NOT guaranteed. Clients needing correlation can use wall-clock timestamps embedded in records.

---

## Schema Management

### Schema WAL (Global)

All schema changes go to a dedicated WAL that grows forever:
```
Schema WAL Entry:
{
  "seqNum": 1,
  "op": "REGISTER",
  "type": "events",
  "version": "v1",
  "schema": {
    "primaryKey": "id",
    "secondaryIndexes": ["user_id", "timestamp"],
    "fields": { ... }
  }
}
```

**Why grow forever?**
- Schema changes are rare (10-100/year typical)
- Each entry is small (<1KB)
- Avoids complexity of tracking "schemas in use"
- 10 years × 100 changes = ~100KB

### Materialized Schema Registry

On startup, replay Schema WAL to build in-memory registry:
```go
type SchemaRegistry struct {
    // (type, version) → Schema
    schemas map[string]map[string]*Schema
    // type → current version
    current map[string]string
}
```

### Schema Structure
```json
{
  "type": "events",
  "version": "v1",
  "primaryKey": "id",
  "secondaryIndexes": ["user_id", "timestamp"],
  "fields": {
    "id": {"type": "string", "required": true},
    "user_id": {"type": "string", "required": true},
    "timestamp": {"type": "int64", "required": true},
    "payload": {"type": "bytes"}
  }
}
```

### Schema Evolution Rules
| Change | Allowed | Notes |
|--------|---------|-------|
| Add optional field | Yes | Old records missing it |
| Add required field with default | Yes | Old records use default |
| Add secondary index | Yes | Only new records indexed (backfill = future) |
| Remove field | Yes | Stop writing, old records keep it |
| Change PK | No | Breaking |
| Change field type | No | Breaking |

---

## Data WAL Design (Per-Type)

### Directory Structure
```
/types/events/
  wal_index              # seqNum range → filename mapping
  000001.wal             # seqNum 1-1000
  000002.wal             # seqNum 1001-2000
  000003.wal             # seqNum 2001-current (active)
```

### WAL Index Format
```json
{
  "files": [
    {"name": "000001.wal", "minSeqNum": 1, "maxSeqNum": 1000, "size": 1048576},
    {"name": "000002.wal", "minSeqNum": 1001, "maxSeqNum": 2000, "size": 1048576},
    {"name": "000003.wal", "minSeqNum": 2001, "maxSeqNum": null, "size": 524288}
  ],
  "activeFile": "000003.wal"
}
```

### WAL Record Format
```json
{
  "seqNum": 12345,
  "op": "UPDATE",
  "schemaVersion": "v2",
  "pk": "evt-123",
  "data": {
    "id": "evt-123",
    "user_id": "alice",
    "timestamp": 1699900000,
    "payload": "..."
  }
}
```

Note: `schemaVersion` is the version name (e.g., "v2"), not a seqNum. This decouples data format from schema storage implementation.

### WAL Pruning

**Pruning rule**: A WAL file can be deleted when ALL consumers have processed past its maxSeqNum.

```go
func (w *WAL) PrunableSeqNum() uint64 {
    return min(
        w.snapshot.LastAppliedSeqNum(),
        w.skLSMs["user_id"].MinUnflushedSeqNum(),
        w.skLSMs["timestamp"].MinUnflushedSeqNum(),
        // ... all SK LSMs
    )
}

func (w *WAL) PruneFiles() {
    prunable := w.PrunableSeqNum()
    for _, file := range w.index.Files {
        if file.MaxSeqNum < prunable {
            os.Remove(file.Name)
            w.index.Remove(file)
        }
    }
}
```

**Key insight**: No consumer tracking for watch clients. Clients own their offset. If their offset is pruned, they re-snapshot.

### Consumers of WAL
1. **State Machine Builder**: Applies ops to in-memory KV (handles PK lookups + OCC)
2. **SK LSM Builders**: Updates (sk, pk) → seqNum indexes
3. **Stream API**: Serves deltas to watch clients (reads directly from WAL files)

---

## State Machine (Snapshot Store)

### Purpose
The State Machine serves three purposes:
1. **Snapshot serving**: Provides current state for Watch API
2. **OCC checks**: Record includes `_updateSeqNum` for conflict detection
3. **Point lookups**: Direct `Get(pk)` for QueryService

### Record Format
```go
type Record struct {
    SchemaVersion string                 // e.g., "v2"
    CreateSeqNum  uint64                 // immutable, set on first create
    UpdateSeqNum  uint64                 // updated on every write
    Data          map[string]interface{} // the actual fields
}
```

### Implementation (Phase 1)
```go
type StateMachine struct {
    mu       sync.RWMutex
    records  map[string]*Record  // pk → Record
    lastSeqNum uint64            // last applied seqNum
}

func (sm *StateMachine) Apply(entry *WALEntry) {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    switch entry.Op {
    case CREATE:
        sm.records[entry.PK] = &Record{
            SchemaVersion: entry.SchemaVersion,
            CreateSeqNum:  entry.SeqNum,
            UpdateSeqNum:  entry.SeqNum,
            Data:          entry.Data,
        }
    case UPDATE:
        rec := sm.records[entry.PK]
        rec.SchemaVersion = entry.SchemaVersion
        rec.UpdateSeqNum = entry.SeqNum
        rec.Data = entry.Data
    case DELETE:
        delete(sm.records, entry.PK)
    }
    sm.lastSeqNum = entry.SeqNum
}
```

### Checkpointing
Periodically write State Machine to disk:
```
/types/events/snapshot/
  checkpoint_5000.json    # State at seqNum 5000
  CURRENT -> checkpoint_5000.json
```

On restart: Load checkpoint + replay WAL from checkpoint's seqNum.

---

## Index LSMs (Secondary Keys Only)

### No Separate PK LSM

Primary key operations go directly to State Machine:
```
Get(pk):     StateMachine.Get(pk) → record
Exists(pk):  StateMachine.Get(pk) != nil
OCC check:   StateMachine.Get(pk).UpdateSeqNum == expectedSeqNum?
```

### SK LSM (one per indexed field)
```
Key:   (sk_field_value, pk) - composite
Value: seqNum (uint64)

Purpose:
- List by field: scan prefix → get candidate pks → fetch from State Machine
- Composite key ensures updates to same record replace old entry
- seqNum used for compaction (newer wins), not for record fetch
```

### Query Flow with Re-check
```go
func (q *QueryService) ListByField(field, value string) ([]*Record, error) {
    // 1. Scan SK LSM for candidates
    candidates := q.skLSM[field].ScanPrefix(value)

    // 2. Fetch from State Machine and re-check
    var results []*Record
    for _, candidate := range candidates {
        record := q.stateMachine.Get(candidate.PK)
        if record == nil {
            continue // deleted
        }
        if record.Data[field] != value {
            continue // SK value changed
        }
        results = append(results, record)
    }
    return results, nil
}
```

The SK LSM is a **hint**, not authoritative. Always re-verify against State Machine.

---

## Admission Checkers

### Interface
```go
type AdmissionChecker interface {
    Check(ctx context.Context, op Operation, record *Record) error
}
```

### Implementations
```go
// Validates record against schema
type SchemaValidator struct {
    registry *SchemaRegistry
}

// Rejects Create if PK already exists
type PKUniquenessChecker struct {
    stateMachine *StateMachine
}

// Rejects Update if expectedSeqNum doesn't match
type OCCChecker struct {
    stateMachine *StateMachine
}

// Rate limiting, quotas, etc.
type RateLimiter struct { ... }
```

### Write Path with Admission
```go
func (s *Server) Write(ctx context.Context, req *WriteRequest) error {
    // 1. Lookup schema
    schema := s.registry.Get(req.Type, req.SchemaVersion)

    // 2. Run admission checkers
    checkers := []AdmissionChecker{
        s.schemaValidator,
        s.pkUniquenessChecker,
        s.occChecker,
    }
    for _, checker := range checkers {
        if err := checker.Check(ctx, req.Op, req.Record); err != nil {
            return err
        }
    }

    // 3. Assign seqNum, append to WAL
    seqNum := s.wal.NextSeqNum()
    entry := &WALEntry{
        SeqNum:        seqNum,
        Op:            req.Op,
        SchemaVersion: req.SchemaVersion,
        PK:            req.Record.PK,
        Data:          req.Record.Data,
    }
    if err := s.wal.Append(entry); err != nil {
        return err
    }

    // 4. Apply to State Machine (synchronous)
    s.stateMachine.Apply(entry)

    // 5. Queue for SK LSM builders (async or sync based on consistency mode)
    s.skBuilder.Enqueue(entry)

    // 6. If strong consistency, wait for SK LSMs
    if req.StrongConsistency {
        s.skBuilder.WaitForSeqNum(seqNum)
    }

    return nil
}
```

---

## Consistency Model

### Write Path Summary
```
1. Client: Write(type, op, record, expectedSeqNum?)
2. Server: Lookup schema from materialized registry
3. Server: Run AdmissionCheckers (schema validation, PK uniqueness, OCC)
4. Server: Assign seqNum, append to type's WAL
5. Server: Apply to State Machine (synchronous, in-memory)
6. Server: Queue for SK LSM builders
7. Server (strong consistency): Wait for SK LSMs to catch up
8. Server: Return success with seqNum
```

### OCC (Optimistic Concurrency Control)
```
Record in State Machine:
  pk: "A"
  CreateSeqNum: 1000
  UpdateSeqNum: 2500

Client wants to update:
  Sends: pk="A", expectedSeqNum=2500, newData={...}

OCCChecker:
  current := StateMachine.Get("A")
  if current.UpdateSeqNum != 2500 → CONFLICT error
  else → allow
```

---

## Watch API (K8s Semantics)

### Protocol
```protobuf
service WatchService {
  rpc Watch(WatchRequest) returns (stream WatchEvent);
}

message WatchRequest {
  string type = 1;
  uint64 from_seq_num = 2;  // 0 = start with snapshot
}

message WatchEvent {
  oneof event {
    Snapshot snapshot = 1;
    Delta delta = 2;
  }
}

message Snapshot {
  uint64 seq_num = 1;
  repeated Resource resources = 2;
}

message Delta {
  uint64 seq_num = 1;
  Operation op = 2;
  Resource resource = 3;
}
```

### Watch Flow
```
1. Client: Watch(type="events", from_seq_num=0)

2. Server (from_seq_num=0, needs snapshot):
   a. Read current state from State Machine
   b. Note the seqNum at snapshot time
   c. Send Snapshot{seq_num=5000, resources=[...]}
   d. Start streaming WAL from seqNum 5001

3. Client reconnects: Watch(type="events", from_seq_num=5001)

4. Server (from_seq_num > 0, resume):
   a. Check: 5001 >= minRetainedSeqNum? (from WAL index)
   b. If yes: stream from WAL at 5001
   c. If no: error "EXPIRED", client must re-snapshot
```

---

## Reference Architectures

| Component | Reference | What to Learn |
|-----------|-----------|---------------|
| WAL | etcd/wal | Segment files, CRC, fsync patterns |
| LSM | Pebble | Memtable, SSTable, compaction |
| Watch | K8s apiserver | Snapshot + bookmark + delta pattern |
| Raft | etcd/raft | Leader election, log replication |

---

## Phased Implementation Plan

### Phase 1: Foundation
- [ ] Project structure + gRPC service definitions
- [ ] Schema WAL + materialized registry
- [ ] Basic data WAL (single file per type, append-only)
- [ ] WAL index
- [ ] In-memory State Machine
- [ ] AdmissionChecker interface + implementations
- [ ] Point lookup via State Machine
- [ ] OCC via State Machine

### Phase 2: Secondary Indexing
- [ ] SK LSM with composite keys (using Pebble or custom)
- [ ] List-by-field queries with re-check
- [ ] Strong consistency mode (wait for SK LSM)

### Phase 3: Watch API
- [ ] Snapshot from State Machine
- [ ] Delta streaming from WAL
- [ ] Client reconnection handling
- [ ] EXPIRED error when offset pruned

### Phase 4: Persistence & Recovery
- [ ] State Machine checkpointing
- [ ] WAL file rotation
- [ ] WAL pruning based on consumer progress
- [ ] Startup recovery (checkpoint + WAL replay)

### Phase 5: Replication (Future)
- [ ] Raft consensus for Schema WAL
- [ ] Raft consensus for data WALs
- [ ] Read replicas

---

## Open Questions (Resolved)

| Question | Resolution |
|----------|------------|
| Global vs per-type WAL | Per-type (like K8s) |
| Schema WAL pruning | Grows forever (changes are rare) |
| Schema reference in data | Version name, not seqNum |
| PK LSM needed? | No, State Machine handles PK ops |
| Cross-type ordering | Not supported, use timestamps if needed |

## Remaining Open Questions

1. **Storage Format**: JSON vs Protobuf?
   - JSON: simpler, human-readable, larger
   - Protobuf: smaller, faster, requires schema compilation

2. **SK LSM Implementation**: Use Pebble or build custom?
   - Pebble: production-ready, but heavy dependency
   - Custom: learning opportunity, full control

---

## Validation Summary

| Aspect | Assessment |
|--------|------------|
| Schema WAL (grows forever) | Sound - rare changes, small entries |
| Per-type data WALs | Sound - matches K8s model |
| WAL pruning (min of consumers) | Sound - standard pattern |
| AdmissionChecker interface | Sound - clean separation |
| State Machine for PK ops | Sound - eliminates complexity |
| SK query with re-check | Sound - index as hint pattern |

---

*Document finalized after architecture discussion*
