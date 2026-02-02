# IndexedWatch - Architecture Document

## Project Vision

A storage engine providing:
- **gRPC API** for schema registration and resource CRUD
- **K8s-style Watch** semantics (snapshot + deltas, per-type ordering)
- **Multi-index lookups** (primary key, secondary keys)
- **Strong consistency** option (writer blocks until indexed)
- **Future**: Raft-based replication for read scaling

---

## Shift-Left Philosophy

**Core principle**: Constrain at schema-definition time, simplify at runtime.

Inspired by Confluent's "shift-left for data streaming" - validate data quality at the source, not downstream. IndexedWatch applies this to stateful workloads:

| Concern | Traditional (Runtime) | IndexedWatch (Registration Time) |
|---------|----------------------|----------------------------------|
| Schema validation | Client's problem | Rejected at write time |
| Type isolation | Client must namespace | Guaranteed by design |
| Cross-type ordering | Client must handle | Not supported (explicit constraint) |
| Index definition | Client builds own | Declared in schema |
| Bad data | Propagates downstream | Rejected early |

**Design consequences**:
- Per-type WAL/SM/LSM isolation (no cross-type interference)
- Schema required before any writes (contract-first)
- Failure blast radius limited to single type
- Simpler runtime: N independent simple systems vs 1 complex global system

---

## Positioning

> **IndexedWatch: Shift-left storage for cloud-native workloads**

| | Kafka | etcd | CockroachDB | **IndexedWatch** |
|--|-------|------|-------------|------------------|
| Schema | Registry (optional) | None | SQL DDL | Required (shift-left) |
| Watch | Consumer groups | Yes | Changefeeds | Yes (K8s-style) |
| Secondary indexes | No | No | Yes | Yes |
| Consistency | Partition-ordered | Linearizable | Serializable | Per-type linearizable |
| Isolation | Topic | Global | Transaction | **Per-type (by design)** |
| Query | Scan only | KV only | Full SQL | PK + indexed fields |

**Target use cases**:
- Internal control planes (like K8s, but for your domain)
- Event-driven microservices (stronger than Kafka, simpler than DB)
- Real-time config/feature flag systems
- Multi-tenant SaaS backends (type-per-tenant isolation)

---

## Production Architecture (Future Vision)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Control Plane                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │  Schema API     │  │  Shard Manager  │  │  Cluster Membership │  │
│  │  - register     │  │  - provision    │  │  - health checks    │  │
│  │  - evolve       │  │  - scale types  │  │  - routing table    │  │
│  │  - validate     │  │  - rebalance    │  │  - leader election  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          Proxy Layer                                 │
│  - Route writes → Leader                                            │
│  - Route reads → Replicas (or Leader for strong consistency)        │
│  - Watch stream fanout                                              │
│  - Connection pooling                                               │
└─────────────────────────────────────────────────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        ▼                         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  Type: events   │       │  Type: users    │       │  Type: orders   │
│                 │       │                 │       │                 │
│  ┌───────────┐  │       │  ┌───────────┐  │       │  ┌───────────┐  │
│  │  Leader   │  │       │  │  Leader   │  │       │  │  Leader   │  │
│  │  (WAL)    │──┼─Raft──┼─▶│ Replica 1 │  │       │  │  (WAL)    │  │
│  └───────────┘  │       │  │ Replica 2 │  │       │  └───────────┘  │
│       │         │       │  └───────────┘  │       │       │         │
│       ▼         │       │                 │       │       ▼         │
│  SM + LSMs      │       │                 │       │  SM + LSMs      │
└─────────────────┘       └─────────────────┘       └─────────────────┘

Leader: Handles writes, replicates via Raft
Replicas: Serve reads, watch streams
Per-type: Independent scaling, failure isolation
```

**Scaling model**:
- Vertical: Larger nodes for hot types
- Horizontal: More replicas for read-heavy types
- Sharding: Split large types across multiple leaders (future)

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

## State Machine (MVCC Snapshot Store)

### Purpose
The State Machine serves four purposes:
1. **Snapshot serving**: Provides current state for Watch API
2. **OCC checks**: Record includes `UpdateSeqNum` for conflict detection
3. **Point lookups**: Direct `Get(pk)` for QueryService
4. **Point-in-time queries**: `GetAt(pk, seqNum)` for consistent SK queries

### Why MVCC?

Without MVCC, SK queries can be inconsistent:
```
Timeline:
  t1: Write(pk=A, user_id=alice) → seqNum=100
  t2: SM updated immediately
  t3: SK LSM still processing seqNum 99...
  t4: Query "user_id=alice" → misses A (SK LSM lag)
```

With MVCC:
- SM stores multiple versions per key
- SK query fetches version at SK LSM's lastAppliedSeqNum
- Consistent point-in-time view guaranteed

### Record Format
```go
type VersionedRecord struct {
    SeqNum        uint64                 // WAL seqNum of this version
    SchemaVersion string                 // e.g., "v2"
    CreateSeqNum  uint64                 // immutable, set on first create
    Data          map[string]interface{} // the actual fields
    IsDeleted     bool                   // tombstone marker
}
```

### MVCC Implementation
```go
type StateMachine struct {
    mu sync.RWMutex

    // pk → versions (sorted by seqNum descending, newest first)
    versions map[string][]*VersionedRecord

    // Minimum seqNum we must retain for consumers
    // = min(all SK LSM lastApplied, oldest active snapshot/watch)
    minRetainedSeqNum uint64

    // Latest seqNum applied
    lastSeqNum uint64
}

// Apply adds a new version for the record
func (sm *StateMachine) Apply(entry *WALEntry) {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    var createSeqNum uint64
    if existing := sm.versions[entry.PK]; len(existing) > 0 {
        createSeqNum = existing[len(existing)-1].CreateSeqNum
    } else {
        createSeqNum = entry.SeqNum
    }

    record := &VersionedRecord{
        SeqNum:        entry.SeqNum,
        SchemaVersion: entry.SchemaVersion,
        CreateSeqNum:  createSeqNum,
        Data:          entry.Data,
        IsDeleted:     entry.Op == DELETE,
    }

    // Prepend (newest first)
    sm.versions[entry.PK] = append(
        []*VersionedRecord{record},
        sm.versions[entry.PK]...,
    )
    sm.lastSeqNum = entry.SeqNum
}

// GetAt returns the record version at or before the given seqNum
func (sm *StateMachine) GetAt(pk string, atSeqNum uint64) *VersionedRecord {
    sm.mu.RLock()
    defer sm.mu.RUnlock()

    for _, v := range sm.versions[pk] {
        if v.SeqNum <= atSeqNum {
            if v.IsDeleted {
                return nil
            }
            return v
        }
    }
    return nil
}

// GetLatest returns the most recent version
func (sm *StateMachine) GetLatest(pk string) *VersionedRecord {
    sm.mu.RLock()
    defer sm.mu.RUnlock()

    if versions := sm.versions[pk]; len(versions) > 0 {
        if versions[0].IsDeleted {
            return nil
        }
        return versions[0]
    }
    return nil
}
```

### Garbage Collection

Old versions are retained until all consumers have processed past them:

```go
func (sm *StateMachine) GC() {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    minRetained := sm.computeMinRetainedSeqNum()

    for pk, versions := range sm.versions {
        // Find cutoff: keep versions >= minRetained, plus one older
        cutoff := len(versions)
        for i, v := range versions {
            if v.SeqNum < minRetained {
                cutoff = i + 1 // Keep this one as floor, remove older
                break
            }
        }
        if cutoff < len(versions) {
            sm.versions[pk] = versions[:cutoff]
        }
    }
}

func (sm *StateMachine) computeMinRetainedSeqNum() uint64 {
    return min(
        sm.skLSMTracker.MinLastApplied(),  // SK LSMs need old versions
        sm.snapshotTracker.MinSeqNum(),    // Active snapshots
        sm.watchTracker.MinSeqNum(),       // Active watch streams
    )
}
```

### Memory Overhead

MVCC memory is bounded:
- GC runs based on `min(consumer progress)`
- Fast consumers = fewer retained versions
- Typically 1-3 versions per key in steady state
- Worst case: slow consumer holds back GC (same as WAL retention)

### Checkpointing

Checkpoint includes all retained versions:
```
/types/events/snapshot/
  checkpoint_5000.json    # Versions with seqNum >= minRetained at 5000
  CURRENT -> checkpoint_5000.json
```

On restart: Load checkpoint + replay WAL from checkpoint's seqNum.

---

## Index LSMs (Secondary Keys Only)

### No Separate PK LSM

Primary key operations go directly to State Machine:
```
Get(pk):         StateMachine.GetLatest(pk) → record
GetAt(pk, seq):  StateMachine.GetAt(pk, seq) → record at seqNum
Exists(pk):      StateMachine.GetLatest(pk) != nil
OCC check:       StateMachine.GetLatest(pk).SeqNum == expectedSeqNum?
```

### SK LSM (one per indexed field)
```
Key:   (sk_field_value, pk) - composite
Value: seqNum (uint64)

Purpose:
- List by field: scan prefix → get candidate pks → fetch versioned record
- Composite key ensures updates to same record replace old entry
- seqNum used for:
  1. Compaction (newer wins)
  2. MVCC lookup (fetch correct version from State Machine)
```

### SK LSM Tracks Progress
```go
type SKLSM struct {
    // ... LSM internals ...

    // Last seqNum fully applied to this LSM
    lastAppliedSeqNum uint64
}
```

This is critical for:
- MVCC queries (query at LSM's lastAppliedSeqNum)
- GC (SM can't delete versions still needed by lagging LSMs)
- WAL pruning (WAL files retained until all LSMs have processed them)

### Query Flow with MVCC
```go
func (q *QueryService) ListByField(field, value string) ([]*Record, error) {
    // 1. Get the seqNum this LSM has processed up to
    //    This ensures consistent point-in-time view
    querySeqNum := q.skLSM[field].LastAppliedSeqNum()

    // 2. Scan SK LSM for candidates
    candidates := q.skLSM[field].ScanPrefix(value)

    // 3. Fetch versioned records from State Machine
    var results []*Record
    for _, candidate := range candidates {
        // Fetch the version that existed when SK entry was written
        // This ensures we see the record as it was when indexed
        record := q.stateMachine.GetAt(candidate.PK, candidate.SeqNum)
        if record == nil {
            continue // deleted at that point
        }
        // Re-check: SK value might have changed between writes
        if record.Data[field] != value {
            continue
        }
        results = append(results, record)
    }
    return results, nil
}
```

### Consistency Guarantee

With MVCC + versioned queries:
- SK query sees consistent snapshot at `skLSM.lastAppliedSeqNum`
- No missing records due to SM/LSM lag
- No phantom reads from concurrent writes

The SK LSM is still a **hint** (re-check needed), but MVCC ensures the hint is evaluated against the correct record version.

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
- [ ] MVCC State Machine (multi-version with GC)
- [ ] AdmissionChecker interface + implementations
- [ ] Point lookup via State Machine (GetLatest, GetAt)
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
| SM/LSM consistency | MVCC in State Machine - query at LSM's lastAppliedSeqNum |

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
| MVCC State Machine | Sound - enables consistent point-in-time queries |
| SK query with MVCC | Sound - query at LSM's seqNum for consistency |
| Version GC | Sound - bounded by slowest consumer |

---

*Document updated with MVCC State Machine design*
