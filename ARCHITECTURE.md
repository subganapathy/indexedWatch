# IndexedWatch - Architecture Document

## Project Vision

A **learning project** to understand WAL, LSM, and Raft consensus. A storage engine providing:
- **gRPC API** for schema registration and resource CRUD
- **K8s-style Watch** semantics (snapshot + deltas, per-type ordering)
- **Multi-index lookups** (hierarchical primary key, secondary keys)
- **Strong consistency** option (writer blocks until indexed)
- **Raft-based replication** for durability and read scaling

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

## Production Architecture (Multi-Replica with Raft)

### Control Plane Overview

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
```

### Per-Type Replication (LSM on Every Replica)

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Replica 1    │     │    Replica 2    │     │    Replica 3    │
│    (Leader)     │     │   (Follower)    │     │   (Follower)    │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│      WAL        │────►│      WAL        │────►│      WAL        │
│  (Raft log)     │     │  (replicated)   │     │  (replicated)   │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│   Primary LSM   │     │   Primary LSM   │     │   Primary LSM   │
│    SK LSMs      │     │    SK LSMs      │     │    SK LSMs      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
   Writes here            Serve reads             Serve reads
   (leader only)         (linearizable            (stale ok)
                          needs leader)
```

This is the standard pattern used by etcd, CockroachDB, and TiKV:
- **Every replica maintains its own LSM/storage**
- **Raft replicates WAL entries** to all replicas
- **Each replica applies WAL** to local LSM independently
- **Reads can go to any replica** (with consistency trade-offs)

### Multi-Type Deployment

```
        ┌─────────────────────────┼─────────────────────────┐
        ▼                         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  Type: events   │       │  Type: users    │       │  Type: orders   │
│  (3 replicas)   │       │  (3 replicas)   │       │  (3 replicas)   │
│                 │       │                 │       │                 │
│  Leader ──Raft──│       │  Leader ──Raft──│       │  Leader ──Raft──│
│    │            │       │    │            │       │    │            │
│    ▼            │       │    ▼            │       │    ▼            │
│  Primary LSM    │       │  Primary LSM    │       │  Primary LSM    │
│  SK LSMs        │       │  SK LSMs        │       │  SK LSMs        │
└─────────────────┘       └─────────────────┘       └─────────────────┘

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
      wal/                          # WAL directory
        wal_index                   # Maps seqNum ranges → files
        000001.wal
        000002.wal
      primary/                      # Primary LSM (pk → record)
        MANIFEST
        000001.sst
        000002.sst
      indexes/
        user_id/                    # SK LSM
        timestamp/                  # SK LSM

    /users/                         # Another type (independent)
      wal/
        wal_index
        000001.wal
      primary/
        MANIFEST
        000001.sst
      indexes/
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
│  QueryService: Get by PK, List by SK, Range queries                         │
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
│  └─────────────────────────────┘  │ │  │  WAL ──→ Primary LSM            │  │
│              │                    │ │  │   │      (pk → record)          │  │
│              ▼                    │ │  │   │      = State Machine        │  │
│  ┌─────────────────────────────┐  │ │  │   │                             │  │
│  │ Materialized Registry       │  │ │  │   └──→ SK LSMs                  │  │
│  │ (type, version) → schema    │  │ │  │        (sk,pk) → seqNum        │  │
│  └─────────────────────────────┘  │ │  └─────────────────────────────────┘  │
└───────────────────────────────────┘ │                                       │
                                      │  ┌─────────────────────────────────┐  │
                                      │  │ Type: "users"                   │  │
                                      │  │  (same structure)               │  │
                                      │  └─────────────────────────────────┘  │
                                      └───────────────────────────────────────┘
```

### Key Insight: Primary LSM IS the State Machine

The Primary LSM serves as the **disk-backed, scalable state machine**:

```
┌─────────────────────────────────────────────────────────────────┐
│              Primary LSM = State Machine                         │
│  (disk-backed, scalable, IS the source of truth)                │
│                                                                  │
│  Key:   hierarchical pk (e.g., "/namespaces/default/pods/nginx")│
│  Value: {                                                        │
│      data:         []byte,      // actual record                │
│      createSeqNum: uint64,                                       │
│      updateSeqNum: uint64,                                       │
│  }                                                               │
│                                                                  │
│  Used for:                                                       │
│    • Uniqueness checks (bloom filters = fast "not exists")      │
│    • Point lookups by PK                                        │
│    • Range queries (hierarchical PKs enable prefix scans)       │
│    • Watch snapshots (point-in-time reads at seqNum)            │
│    • Optimistic concurrency (check updateSeqNum)                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Secondary LSMs                              │
│  Key:   (sk_value, pk)                                          │
│  Value: seqNum                                                   │
│                                                                  │
│  Used for:                                                       │
│    • Query by secondary key                                      │
│    • Get PKs, then fetch from Primary LSM for actual values     │
└─────────────────────────────────────────────────────────────────┘
```

**Why disk-backed instead of in-memory?**
- Scales beyond RAM limits
- Bloom filters provide fast "not exists" checks for admission
- LSM snapshots enable consistent point-in-time reads
- Standard pattern used by etcd, CockroachDB, TiKV

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
        w.primaryLSM.LastAppliedSeqNum(),    // Primary LSM
        w.skLSMs["user_id"].LastAppliedSeqNum(),
        w.skLSMs["timestamp"].LastAppliedSeqNum(),
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
1. **Primary LSM**: Applies ops to disk-backed state (handles PK lookups, range queries, OCC)
2. **SK LSM Builders**: Updates (sk, pk) → seqNum indexes
3. **Stream API**: Serves deltas to watch clients (reads directly from WAL files)

---

## Primary LSM as State Machine

### Purpose
The Primary LSM serves as the **disk-backed state machine** with five purposes:
1. **Uniqueness checks**: Bloom filters for fast "not exists" (admission control)
2. **Point lookups**: Direct `Get(pk)` for QueryService
3. **Range queries**: Prefix scans on hierarchical PKs
4. **Snapshot serving**: Point-in-time reads for Watch API
5. **OCC checks**: Record includes `UpdateSeqNum` for conflict detection

### Why Disk-Backed Instead of In-Memory?

In-memory state machines don't scale:
- Limited by RAM
- Requires complex checkpointing
- Recovery means replaying entire WAL

With disk-backed LSM:
- Scales to dataset size (not limited by RAM)
- Bloom filters provide fast "not exists" checks
- LSM snapshots enable consistent point-in-time reads
- Standard pattern used by etcd, CockroachDB, TiKV

### Record Format in Primary LSM
```go
// Key: hierarchical pk (e.g., "/namespaces/default/pods/nginx")
// Value:
type StoredRecord struct {
    Data          []byte  // actual record (JSON/protobuf)
    SchemaVersion string  // e.g., "v2"
    CreateSeqNum  uint64  // immutable, set on first create
    UpdateSeqNum  uint64  // updated on every write
    IsDeleted     bool    // tombstone marker
}
```

### Hierarchical Primary Keys

PKs are hierarchical (like K8s) to enable range queries:
```
/namespaces/default/pods/nginx
/namespaces/default/pods/redis
/namespaces/prod/pods/api
/namespaces/prod/pods/web
```

Range query: "List all pods in namespace `default`"
→ Prefix scan on `/namespaces/default/pods/`

### Write Flow

```
Record A created (pk="/namespaces/default/pods/nginx"):
┌─────────────────────────────────────────────────────────────────┐
│ 1. AdmissionChecker: Does pk exist in Primary LSM?              │
│    └─► Bloom filter says NO → proceed (fast path)               │
│ 2. Write to WAL → assigned seqNum=1                             │
│ 3. WAL entry applied to Primary LSM and SK LSMs                 │
│ 4. API blocks until Primary LSM reflects seqNum=1               │
│ 5. Return success                                               │
└─────────────────────────────────────────────────────────────────┘

Record A updated:
┌─────────────────────────────────────────────────────────────────┐
│ 1. AdmissionChecker: Get A's updateSeqNum from Primary LSM      │
│    └─► Check: client's expectedSeqNum == LSM's updateSeqNum?    │
│        (optimistic concurrency control)                         │
│ 2. Write to WAL → assigned seqNum=2                             │
│ 3. WAL entry applied to Primary LSM and SK LSMs                 │
│ 4. API blocks until Primary LSM reflects seqNum=2               │
│ 5. Return success                                               │
└─────────────────────────────────────────────────────────────────┘
```

### LSM Snapshot for Watches

```
Watch request starting at seqNum=5:

1. Take LSM snapshot at current seqNum (consistent point-in-time view)
2. Stream current state to client (initial snapshot)
3. Subscribe to WAL for seqNum > snapshot's seqNum
4. Stream deltas as they arrive from WAL
```

### MVCC for SK Query Consistency

SK LSMs may lag behind Primary LSM. To ensure consistency:

```go
func (q *QueryService) ListByField(field, value string) ([]*Record, error) {
    // 1. Get the seqNum this SK LSM has processed up to
    querySeqNum := q.skLSM[field].LastAppliedSeqNum()

    // 2. Scan SK LSM for candidates
    candidates := q.skLSM[field].ScanPrefix(value)

    // 3. Fetch records from Primary LSM at querySeqNum
    var results []*Record
    for _, candidate := range candidates {
        // Use LSM snapshot at querySeqNum for consistency
        record := q.primaryLSM.GetAt(candidate.PK, querySeqNum)
        if record == nil || record.IsDeleted {
            continue
        }
        // Re-check: SK value might have changed
        if record.Data[field] != value {
            continue
        }
        results = append(results, record)
    }
    return results, nil
}
```

---

## Index LSMs

### Primary LSM (stores actual records)

Primary key operations go to Primary LSM:
```
Get(pk):         PrimaryLSM.Get(pk) → record
GetAt(pk, seq):  PrimaryLSM.GetAt(pk, seq) → record at seqNum
Exists(pk):      PrimaryLSM.Get(pk) != nil (bloom filter fast path)
OCC check:       PrimaryLSM.Get(pk).UpdateSeqNum == expectedSeqNum?
Range(prefix):   PrimaryLSM.Scan(prefix) → records (hierarchical PKs)
```

### SK LSM (one per indexed field)
```
Key:   (sk_field_value, pk) - composite
Value: seqNum (uint64)

Purpose:
- List by field: scan prefix → get candidate pks → fetch from Primary LSM
- Composite key ensures updates to same record replace old entry
- seqNum used for:
  1. Compaction (newer wins)
  2. MVCC lookup (fetch correct version from Primary LSM)
```

### All LSMs Track Progress
```go
type LSM struct {
    // ... LSM internals ...

    // Last seqNum fully applied to this LSM
    lastAppliedSeqNum uint64
}
```

This is critical for:
- MVCC queries (query at LSM's lastAppliedSeqNum)
- WAL pruning (WAL files retained until all LSMs have processed them)

### Consistency Guarantee

With MVCC + versioned queries:
- SK query sees consistent snapshot at `skLSM.lastAppliedSeqNum`
- No missing records due to Primary/SK LSM lag
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

// Rejects Create if PK already exists (uses Primary LSM bloom filter)
type PKUniquenessChecker struct {
    primaryLSM *LSM
}

// Rejects Update if expectedSeqNum doesn't match
type OCCChecker struct {
    primaryLSM *LSM
}

// Rate limiting, quotas, etc.
type RateLimiter struct { ... }
```

### Write Path with Admission
```go
func (s *Server) Write(ctx context.Context, req *WriteRequest) error {
    // 1. Lookup schema
    schema := s.registry.Get(req.Type, req.SchemaVersion)

    // 2. Run admission checkers (checks Primary LSM)
    //    - PKUniquenessChecker uses bloom filter for fast "not exists"
    //    - OCCChecker reads updateSeqNum from Primary LSM
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

    // 4. WAL entry applied to Primary LSM and SK LSMs
    //    (can be async with write-ahead guarantee from WAL)
    s.primaryLSM.Apply(entry)
    s.skBuilder.Enqueue(entry)

    // 5. Block until Primary LSM reflects the write
    s.primaryLSM.WaitForSeqNum(seqNum)

    // 6. If strong consistency, also wait for SK LSMs
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
3. Server: Run AdmissionCheckers against Primary LSM
   - Schema validation
   - PK uniqueness (bloom filter fast path)
   - OCC check (compare updateSeqNum)
4. Server: Assign seqNum, append to type's WAL
5. Server: Apply to Primary LSM and SK LSMs
6. Server: Block until Primary LSM reflects the write
7. Server (strong consistency): Also wait for SK LSMs
8. Server: Return success with seqNum
```

### OCC (Optimistic Concurrency Control)
```
Record in Primary LSM:
  pk: "/namespaces/default/pods/nginx"
  CreateSeqNum: 1000
  UpdateSeqNum: 2500

Client wants to update:
  Sends: pk="/namespaces/default/pods/nginx", expectedSeqNum=2500, newData={...}

OCCChecker:
  current := PrimaryLSM.Get(pk)
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
   a. Take LSM snapshot at current seqNum
   b. Stream current state from Primary LSM snapshot
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

### Phase 1: Foundation (Current)
- [x] Project structure + gRPC service definitions
- [x] Generic WAL implementation (PR #2)
  - [x] 8-byte aligned frames
  - [x] CRC32-C checksums
  - [x] PageWriter for buffered I/O
  - [x] Segment rotation
- [ ] Schema WAL + materialized registry
- [ ] WAL extensions: seqNum tracking, TruncateBefore(), ReadFrom()

### Phase 2: Primary LSM
- [ ] Primary LSM implementation (using Pebble or custom)
  - [ ] Hierarchical PK support
  - [ ] Bloom filters for fast "not exists"
  - [ ] Point lookups and range scans
  - [ ] MVCC snapshots
- [ ] AdmissionChecker interface + implementations
- [ ] OCC via Primary LSM

### Phase 3: Secondary Indexing
- [ ] SK LSM with composite keys
- [ ] List-by-field queries with re-check
- [ ] Strong consistency mode (wait for SK LSM)

### Phase 4: Watch API
- [ ] Snapshot from Primary LSM
- [ ] Delta streaming from WAL
- [ ] Client reconnection handling
- [ ] EXPIRED error when offset pruned

### Phase 5: Persistence & Recovery
- [ ] WAL file rotation
- [ ] WAL pruning based on consumer progress (all LSMs)
- [ ] Startup recovery (WAL replay to LSMs)

### Phase 6: Replication
- [ ] Raft consensus for Schema WAL
- [ ] Raft consensus for data WALs
- [ ] LSM on every replica
- [ ] Read replicas with consistency options

---

## Open Questions (Resolved)

| Question | Resolution |
|----------|------------|
| Global vs per-type WAL | Per-type (like K8s) |
| Schema WAL pruning | Grows forever (changes are rare) |
| Schema reference in data | Version name, not seqNum |
| PK LSM needed? | Yes, Primary LSM = State Machine (disk-backed, scalable) |
| Cross-type ordering | Not supported, use timestamps if needed |
| SM/LSM consistency | MVCC via LSM snapshots - query at SK LSM's lastAppliedSeqNum |
| In-memory vs disk state | Disk-backed (Primary LSM) - scales beyond RAM |
| Multi-replica scaling | LSM on every replica (standard Raft pattern) |

## Remaining Open Questions

1. **Storage Format**: JSON vs Protobuf?
   - JSON: simpler, human-readable, larger
   - Protobuf: smaller, faster, requires schema compilation

2. **LSM Implementation**: Use Pebble or build custom?
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
| Primary LSM = State Machine | Sound - disk-backed, scalable, bloom filters for admission |
| Hierarchical PKs | Sound - enables range queries ("list all in prefix") |
| SK query with MVCC | Sound - query at SK LSM's seqNum for consistency |
| LSM on every replica | Sound - standard Raft pattern (etcd, CockroachDB, TiKV) |

---

*Document updated with Primary LSM = State Machine architecture*
