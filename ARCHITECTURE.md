# IndexedWatch - Architecture Document

## Project Vision

A **learning project** to deeply understand WAL, LSM, leader election, and replicated storage.
A storage engine providing:
- **gRPC API** for schema registration and resource CRUD
- **K8s-style Watch** semantics (snapshot + deltas, per-type ordering)
- **Multi-index lookups** (hierarchical primary key, secondary keys)
- **ISR-based replication** with leader election for durability and read scaling
- **Strong or eventual consistency** (client's choice)

---

## Shift-Left Philosophy

**Core principle**: Constrain at schema-definition time, simplify at runtime.

Inspired by Confluent's "shift-left for data streaming" - validate data quality at the source,
not downstream. IndexedWatch applies this to stateful workloads:

| Concern | Traditional (Runtime) | IndexedWatch (Registration Time) |
|---------|----------------------|----------------------------------|
| Schema validation | Client's problem | Rejected at write time |
| Type isolation | Client must namespace | Guaranteed by design |
| Cross-type ordering | Client must handle | Not supported (explicit constraint) |
| Index definition | Client builds own | Declared in schema |
| Bad data | Propagates downstream | Rejected early |

**Design consequences**:
- Per-type WAL/LSM isolation (no cross-type interference)
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
| Replication | ISR (Kafka protocol) | Raft per write | Raft per write | **ISR (Kafka-style)** |

**Target use cases**:
- Internal control planes (like K8s, but for your domain)
- Event-driven microservices (stronger than Kafka, simpler than DB)
- Real-time config/feature flag systems
- Multi-tenant SaaS backends (type-per-tenant isolation)

---

## Production Architecture

### Two Separate Protocols

IndexedWatch separates **coordination** from **data replication**:

1. **Leader Election**: Elects one leader per resource type. Leader is the single writer.
   Uses a Raft-based protocol for leader election and failure detection.
2. **ISR Replication**: Leader ships WAL entries to followers in batches via raw TCP
   with sendfile() zero-copy. Kafka's model — simpler and higher throughput than
   Raft-per-write.

This is the same separation Kafka used with ZooKeeper: consensus for coordination,
a custom protocol for high-throughput data replication.

### Why Not Raft Per Write?

| Approach | Throughput | Complexity | Used By |
|----------|-----------|------------|---------|
| Raft per write | Bounded by Raft consensus RTT | Log matching, term checking per entry | etcd, TiKV, CockroachDB |
| ISR replication | Bounded by network bandwidth (batched) | Simpler: sequential stream + ack | Kafka |
| Async replication | Highest (no ack wait) | Risk of data loss | Redis |

We chose ISR for throughput. Every write is durably replicated to a majority before the
client gets an ack, but the replication is batched — hundreds of writes per network round trip.

### Per-Type Replication

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Replica 1    │     │    Replica 2    │     │    Replica 3    │
│    (Leader)     │     │   (Follower)    │     │   (Follower)    │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│   Shared WAL    │────►│   Shared WAL    │────►│   Shared WAL    │
│  (replicated    │ raw │  (replicated)   │ raw │  (replicated)   │
│   via ISR)      │ TCP │                 │ TCP │                 │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│  Primary LSM    │     │  Primary LSM    │     │  Primary LSM    │
│  SK LSMs        │     │  SK LSMs        │     │  SK LSMs        │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│  pending_set    │     │                 │     │                 │
│  PKLocker       │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
   Writes here          Serve eventual reads    Serve eventual reads
   + strong reads       (may be stale)          (may be stale)
```

- **Every replica maintains its own LSM/storage**
- **Leader replicates WAL entries** to followers via ISR protocol
- **Each replica applies committed WAL entries** to local LSMs independently
- **Reads can go to any replica** (with consistency trade-offs)
- **pending_set + PKLocker** exist only on the leader

### Multi-Type Deployment

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  Type: events   │       │  Type: users    │       │  Type: orders   │
│  (3 replicas)   │       │  (3 replicas)   │       │  (3 replicas)   │
│                 │       │                 │       │                 │
│  Leader ──ISR──►│       │  Leader ──ISR──►│       │  Leader ──ISR──►│
│    │            │       │    │            │       │    │            │
│    ▼            │       │    ▼            │       │    ▼            │
│  Primary LSM    │       │  Primary LSM    │       │  Primary LSM    │
│  SK LSMs        │       │  SK LSMs        │       │  SK LSMs        │
└─────────────────┘       └─────────────────┘       └─────────────────┘

Per-type: Independent scaling, failure isolation, separate leaders
```

**Scaling model**:
- Vertical: Larger nodes for hot types
- Horizontal: More replicas for read-heavy types
- Cross-type: Fully parallel (separate leaders, separate WALs)

---

## Storage Layout

```
/data/
  /schemas/                         # Global schema WAL (grows forever)
    wal_index
    000001.wal
    materialized/
      events/
        v1.json
        CURRENT -> v1

  /types/
    /events/                        # Per-type storage (independent)
      wal/                          # Shared WAL (replicated, feeds all LSMs)
        wal_index
        000001.wal                  # Segment files
        000002.wal
        hwm                         # Persisted high water mark (uint64)
      primary/                      # Primary LSM (pk → record)
        MANIFEST
        000001.sst
      indexes/
        user_id/                    # SK LSM (sk,pk → seqNum)
        timestamp/                  # SK LSM
```

### Key Change: Shared WAL

One WAL per type feeds **all** LSMs (Primary + every SK LSM). No separate application WAL.
The WAL is the LSM's WAL — it provides durability for the memtable and is the replication
stream to followers.

Each LSM tracks its own `lastAppliedSeqNum`. On crash recovery, each LSM independently
replays the WAL from its `lastAppliedSeqNum` to the persisted HWM.

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
- 10 years × 100 changes = ~100KB

### Materialized Schema Registry

On startup, replay Schema WAL to build in-memory registry:
```go
type SchemaRegistry struct {
    schemas map[string]map[string]*Schema  // (type, version) → Schema
    current map[string]string              // type → current version
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

## Write Path (Leader)

### Overview

```
Per-PK lock {
  1. Admission + OCC check (against LSM memtable + pending_set)
  2. WAL append (local fsync, batched group commit)
  3. Add entry to pending_set
  Unlock
}
4. Entry queued into replication batch
5. Batch sent to followers (raw TCP + sendfile)
6. Majority ack → HWM advances → entry committed
7. Apply to Primary LSM memtable
8. Apply to SK LSMs (synchronous, just memtable inserts)
9. Remove from pending_set
10. Return seqNum to client (was blocked waiting for HWM >= seqNum)
```

### Key Properties

- **Per-PK lock held only for local ops** (µs) — steps 1-3
- **Same-PK writes serialize through commit** — cannot have two concurrent updates
  to the same PK. Second write blocks until first completes.
- **Cross-PK writes fully parallel** — thousands of different PKs batch together
  in one replication round
- **Memtable only contains committed data** — WAL is the staging area
- **Shared WAL** — one WAL feeds Primary LSM + all SK LSMs
- **SK LSMs don't need their own WAL** — rebuilt from shared WAL on crash

### PKLocker (Sharded Mutex)

Serializes admission + OCC + WAL append per PK. Industry standard — every major
database serializes uniqueness checks:

| System | Serialization Mechanism |
|--------|------------------------|
| PostgreSQL | B-tree page lock |
| MySQL/InnoDB | Record lock on clustered index |
| RocksDB TransactionDB | Lock table (16 sharded stripes) |
| etcd | Serial apply loop |
| **IndexedWatch** | **Sharded PKLocker (256 stripes)** |

```go
type PKLocker struct {
    shards [256]sync.Mutex
}

func (l *PKLocker) Lock(pk []byte)   { l.shards[hash(pk)&255].Lock() }
func (l *PKLocker) Unlock(pk []byte) { l.shards[hash(pk)&255].Unlock() }
```

Fixed memory. No cleanup. Cross-PK parallelism up to 256-way. False contention
~1/256 per concurrent pair — negligible.

### pending_set

In-memory structure tracking writes between WAL append and commit. Prevents TOCTOU:
without it, two concurrent CREATEs for the same PK could both pass the bloom filter
check (LSM doesn't have it yet) and both get written.

Two access patterns:
1. **Point query by PK** (O(1)) — admission/OCC: "is there a pending write for this PK?"
2. **Drain by seqNum ≤ HWM** (O(k)) — commit loop: apply entries to LSMs

```go
type PendingSet struct {
    mu    sync.Mutex
    byPK  map[string]*PendingEntry   // point lookup
    queue []*PendingEntry            // FIFO ordered by seqNum
}
```

Naturally FIFO because WAL assigns seqNums monotonically. Small and bounded
(only in-flight entries between WAL append and commit).

### Admission + OCC

```
CREATE: bloom filter says "doesn't exist" in LSM + not in pending_set → allow
UPDATE: LSM.Get(pk).UpdateSeqNum == expectedSeqNum + not in pending_set → allow
```

If pending_set has the PK → reject immediately (conflict with in-flight write).

### Commit Loop (Background Goroutine)

```go
for {
    newHWM := computeHWM()  // min(acked offset across ISR majority)
    if newHWM > currentHWM {
        for seq := currentHWM + 1; seq <= newHWM; seq++ {
            entry := wal.Read(seq)
            primaryLSM.Apply(entry)
            for _, skLSM := range skLSMs {
                skLSM.Apply(entry)
            }
            pendingSet.Remove(entry.PK)
        }
        currentHWM = newHWM
        commitNotify.Broadcast()  // wake up waiting clients
    }
}
```

Clients block in `waitForCommit(seqNum)` until `currentHWM >= seqNum`.

---

## Primary LSM

### Purpose

The Primary LSM is the **disk-backed state machine** — the source of truth for all
committed data:

```
Key:   hierarchical pk (e.g., "/namespaces/default/pods/nginx")
Value: StoredRecord {
    Data          []byte   // actual record
    SchemaVersion string   // e.g., "v2"
    CreateSeqNum  uint64   // immutable, set on first create
    UpdateSeqNum  uint64   // updated on every write
    IsDeleted     bool     // tombstone marker (logical delete)
}
```

Five purposes:
1. **Uniqueness checks**: Bloom filters for fast "not exists" (admission)
2. **Point lookups**: Direct `Get(pk)` for reads
3. **Range queries**: Prefix scans on hierarchical PKs
4. **OCC checks**: Compare `UpdateSeqNum` against client's `expectedSeqNum`
5. **Watch snapshots**: Point-in-time consistent reads

### Hierarchical Primary Keys

```
/namespaces/default/pods/nginx
/namespaces/default/pods/redis
/namespaces/prod/pods/api
```

Range query: "List all pods in namespace `default`" → prefix scan on
`/namespaces/default/pods/`

### Why Disk-Backed?

- Scales beyond RAM limits
- Bloom filters provide fast "not exists" checks
- Standard pattern used by etcd, CockroachDB, TiKV

---

## Secondary Index LSMs

One SK LSM per indexed field:

```
Key:   (sk_field_value, pk) — composite
Value: seqNum (uint64)
```

### Query Path

```go
func ListByField(field, value string) []*Record {
    candidates := skLSM[field].ScanPrefix(value)  // get candidate PKs
    var results []*Record
    for _, c := range candidates {
        record := primaryLSM.Get(c.PK)
        if record == nil || record.IsDeleted { continue }
        if record.Data[field] != value { continue }  // re-check (SK is a hint)
        results = append(results, record)
    }
    return results
}
```

SK LSMs are **synchronously updated** after commit (step 8 in write path).
Just a memtable insert — microseconds. No separate WAL needed. On crash,
rebuilt from shared WAL.

---

## Replication Protocol (Kafka ISR-style)

### Leader Side

```
Leader:
  1. Writes to local WAL (fsync, group commit)
  2. Batches entries for replication (size threshold + time threshold)
  3. Sends batch to each follower (raw TCP, sendfile zero-copy)
  4. Tracks each follower's acknowledged offset
  5. HWM = min(acked offset across ISR majority)
  6. Piggybacks HWM on every batch sent to followers
```

### Follower Side

```
Follower:
  1. Receives batch over persistent TCP connection
  2. Validates leader epoch (reject stale leaders)
  3. Persists WAL entries to local WAL file + fsync
  4. Persists HWM to local disk
  5. Applies committed entries (seqNum ≤ HWM) to local LSMs
  6. Sends ack: "I have persisted through seqNum X"
```

### ISR Management

- Follower falls behind (ack lag > threshold) → removed from ISR
- Follower catches up → rejoined to ISR
- ISR < `min.insync.replicas` → leader rejects new writes
- Leader self-demotes if can't reach ISR majority within timeout (CheckQuorum)

### Ordering Guarantee

WAL is sequential (seqNum 1, 2, 3...). Single ordered TCP stream per follower.
Followers apply entries in order — can't apply seqNum 5 without having 4.
HWM = highest **contiguous** committed seqNum. Out-of-order commits are
structurally impossible.

### Replication Transport: Raw TCP + sendfile()

True zero-copy: data never enters user space. Kernel transfers directly from
page cache to NIC via `sendfile()` syscall.

**Normal transfer (4 copies):**
```
Disk → Kernel Page Cache → User Buffer → Socket Buffer → NIC
         (2 unnecessary CPU copies through user space)
```

**sendfile() (2 copies):**
```
Disk → Kernel Page Cache → NIC
         (zero CPU copies, DMA only)
```

**Wire protocol:**
```
Leader → Follower:
  Header (32 bytes, regular write):
    magic(4) | leaderEpoch(8) | startSeqNum(8) | entryCount(4) | payloadSize(4) | hwm(8)
  Payload (sendfile, zero-copy):
    raw WAL bytes from segment file

Follower → Leader:
  Ack (8 bytes): ackedSeqNum
```

**Go implementation:**
```go
// sendfile: io.Copy from *os.File to *net.TCPConn triggers sendfile() on Linux
walFile.Seek(batch.offset, io.SeekStart)
io.Copy(tcpConn, io.LimitReader(walFile, int64(batch.size)))

// Direct syscall for full control:
syscall.Sendfile(socketFd, fileFd, &offset, count)
```

### Batching

Entries accumulate in a replication buffer. Batch sent when:
- **Size threshold**: batch reaches N bytes (like Kafka's `batch.size`)
- **Time threshold**: timer fires after M milliseconds (like Kafka's `linger.ms`)

Hundreds of writes per network round trip.

---

## Consistency Model

### Read Path — Proxy-Based Routing

```
Client → Proxy:
  Read(pk, consistency=STRONG)     → route to leader (linearizable)
  Read(pk, consistency=EVENTUAL)   → route to any follower (may be stale)
  Write(...)                       → always route to leader
```

### Linearizable Reads on Leader

A partitioned leader doesn't know it's been deposed. Two options:

1. **ReadIndex** (etcd's approach): Leader sends heartbeat to ISR majority before
   serving the read. Confirms it still holds leadership. One RTT cost per read.
   No clock assumptions. Safe under arbitrary clock drift.

2. **Leader Lease** (TiKV's approach): Time-based lease (e.g., 9s lease, 10s election
   timeout). Leader serves reads without heartbeat during lease window. Faster but
   relies on bounded clock drift. TiKV uses `CLOCK_MONOTONIC_RAW`.

### Follower Reads

Followers apply committed entries to local LSMs — lag behind leader by replication delay.
Client options (like K8s `resourceVersion`):
- Read from any follower (possibly stale, fast)
- Specify `minSeqNum` (follower waits until its applied seqNum ≥ minSeqNum)

### OCC (Optimistic Concurrency Control)

```
Record in Primary LSM:
  pk: "/namespaces/default/pods/nginx"
  UpdateSeqNum: 2500

Client update:
  Sends: pk, expectedSeqNum=2500, newData={...}

Admission:
  1. PKLocker.Lock(pk)
  2. Check pending_set: pk not present ✓
  3. Check LSM: record.UpdateSeqNum == 2500 ✓
  4. WAL append → seqNum=2501
  5. Add to pending_set
  6. PKLocker.Unlock(pk)
  7. Wait for commit (HWM ≥ 2501)
  8. Return seqNum=2501
```

---

## Partition Protection & Fencing

### Safety Mechanisms

| Concern | Mechanism | Inspired By |
|---------|-----------|-------------|
| Zombie leader writes | Quorum: can't advance HWM without majority ISR ack | Kafka |
| Zombie leader reads | ReadIndex (heartbeat) or Leader Lease (time-based) | etcd, TiKV |
| Leader self-demotion | CheckQuorum: can't reach ISR majority → step down | etcd Raft |
| Stale leader after rejoin | Leader epoch: followers/clients reject stale epoch | Kafka, Raft |
| Follower HWM after crash | Each follower persists HWM locally alongside WAL | Kafka |
| New leader after failover | Truncate WAL to last known HWM; drop uncommitted entries | Kafka |

### Fencing Token: Leader Epoch

Derived from the election protocol (monotonically increasing on each election).
Included in every WAL entry, replication batch, and client response.

- Followers reject WAL entries from a stale epoch
- Clients reject responses from a stale epoch
- Universal pattern: etcd (term), Kafka (leader epoch), CockroachDB (liveness epoch)

### Failure Scenarios

**Replication stalls (followers slow/partitioned):**
- WAL accumulates locally on leader (bounded by disk)
- Memtable stays clean (only committed data)
- Clients timeout waiting for commit → error
- ISR shrinks below minimum → leader rejects new writes
- Recovery: replication resumes → HWM advances → clients unblocked

**Leader crashes:**
- Election protocol elects new leader from ISR
- New leader has all committed entries (was in ISR, acked ≥ HWM)
- Uncommitted entries in old leader's WAL are lost (clients timed out)

**Old leader rejoins:**
- Discovers new leader epoch, steps down
- Truncates local WAL to its persisted HWM (drops uncommitted entries)
- Syncs from new leader as a follower

**Stale leader serves reads (network partition):**
- ReadIndex: leader must confirm with ISR majority before serving → fails if partitioned
- Leader Lease: lease expires within election timeout → stops serving
- CheckQuorum: leader self-demotes if can't reach ISR majority

---

## Crash Recovery

### Leader or Follower Restart

1. Read persisted HWM from disk
2. Primary LSM recovers from its own state (memtable from WAL replay up to HWM)
3. Each SK LSM checks its `lastAppliedSeqNum`:
   - If `lastAppliedSeqNum < HWM` → replay shared WAL entries from there to HWM
4. Rebuild pending_set as empty (no in-flight writes after restart)
5. If leader: resume accepting writes from HWM + 1
6. If follower: reconnect to leader, resume streaming from local WAL offset

### Why This Is Simple

The shared WAL is the single source of truth. Each LSM tracks its own progress.
On crash, each LSM independently catches up to HWM. Same mechanism for Primary
and SK LSMs — just different starting offsets.

---

## Watch API (Phase 5 — TBD)

Watch is a tractable addition after the core write path and replication are built.

### Approach

After Phase 4 (snapshot + pruning), all the plumbing exists:
- Committed entries flow through the shared WAL
- LSM has all committed state
- Ring buffer (existing implementation) populated after commit (step 9 in write path)

Watch = snapshot LSM at current HWM + stream committed entries from HWM+1 via ring buffer.

### Protocol (K8s Semantics)
```protobuf
service WatchService {
  rpc Watch(WatchRequest) returns (stream WatchEvent);
}

message WatchRequest {
  string type = 1;
  uint64 from_seq_num = 2;  // 0 = start with snapshot
}
```

### Follower-Served Watch

Followers can serve watch from their local committed state. Snapshot at follower's
applied seqNum, stream as new entries are applied. Same pattern, different node.

---

## Reference Architectures

| Component | Reference | What We Learn |
|-----------|-----------|---------------|
| WAL | etcd/wal, Kafka log | Segment files, CRC, fsync, sendfile |
| LSM | Pebble | Arena, skiplist, memtable, SSTable, compaction |
| Leader Election | etcd/raft | Term-based election, CheckQuorum, PreVote |
| Replication | Kafka ISR | HWM, batching, ISR management, zero-copy |
| Watch | K8s apiserver | Snapshot + bookmark + delta pattern |
| Consistency | etcd, TiKV | ReadIndex, Leader Lease |
| Fencing | Kafka, CockroachDB | Leader epoch, liveness epoch |
| PK Uniqueness | RocksDB, PostgreSQL | Lock tables, B-tree page locks |

---

## Phased Implementation Plan

### Phase 1: LSM Subsystem
- [x] Arena allocator (Pebble-style, with overflow trick)
- [x] Skiplist (lock-free CAS, MVCC trailer, doubly-linked)
- [x] Memtable (skiplist wrapper with seqNum tracking)
- [x] SSTable writer + reader (block-based, bloom filters)
- [x] LSM engine (multi-level compaction L0→L6)
- [ ] Add WAL inside LSM engine (replaces separate application WAL)

### Phase 2: Single-Node Write Path
- [ ] PKLocker (sharded mutex, 256 stripes)
- [ ] pending_set (FIFO queue + PK map)
- [ ] Write path: admission → OCC → WAL append → pending_set → commit → apply
- [ ] SK LSM integration (synchronous apply after commit)
- [ ] Crash recovery (replay shared WAL to HWM)

### Phase 3: Replication
- [ ] Leader election protocol
- [ ] Raw TCP replication server (custom binary framing)
- [ ] sendfile() zero-copy WAL transfer
- [ ] ISR tracking, HWM management, commit loop
- [ ] Leader epoch fencing
- [ ] Follower: receive WAL → persist → ack → apply to local LSMs
- [ ] Failure handling: leader crash, follower rejoin, WAL truncation

### Phase 4: Consistency + Operations
- [ ] Proxy: strong reads → leader, eventual reads → follower
- [ ] ReadIndex or Leader Lease for linearizable reads
- [ ] CheckQuorum: leader self-demotion on ISR loss
- [ ] Snapshot-to-file + WAL pruning (considers HWM + all LSM progress)

### Phase 5: Watch API
- [ ] Ring buffer populated after commit
- [ ] Snapshot from Primary LSM at HWM
- [ ] Delta streaming from ring buffer
- [ ] Client reconnection (EXPIRED handling)
- [ ] Follower-served watch

---

## Design Decisions Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Replication protocol | ISR (Kafka-style), not Raft per write | Higher throughput via batching |
| Number of WALs | One shared WAL per type | Eliminates redundant durability, simplifies recovery |
| Apply timing | After commit (HWM advance) | Memtable stays clean; no dirty reads |
| SK update timing | Synchronous after commit | Just memtable insert (µs); no separate WAL needed |
| PK serialization | Sharded mutex (256 stripes) | Industry standard; simple, bounded memory |
| Conflict detection | pending_set (FIFO + PK map) | Prevents TOCTOU between admission and commit |
| Replication transport | Raw TCP + sendfile() | True zero-copy; learning opportunity |
| Leader election | Build ourselves | Learning opportunity |
| Strong reads | ReadIndex or Leader Lease | Prevents zombie leader from serving stale data |

### Alternatives Considered

| Alternative | Why Rejected |
|------------|-------------|
| Raft per write | Overhead per entry; lower throughput than ISR batching |
| Leader-only durability | Leader crash = data loss; not production-grade |
| Proposal WAL | Contains entries that may abort; breaks watch, complicates replay |
| Two WALs (app + LSM) | Redundant durability; unnecessary complexity |
| Apply before commit | Dirty data in memtable if replication fails |
| Use etcd for leader election | Want to learn leader election; build everything ourselves |
| gRPC for replication | Can't do sendfile(); want to learn raw TCP + zero-copy |

---

## Resolved Questions

| Question | Resolution |
|----------|------------|
| Global vs per-type WAL | Per-type (K8s model) |
| Schema WAL pruning | Grows forever (changes rare, entries small) |
| How many WALs per type | One shared WAL (feeds Primary + all SK LSMs) |
| Apply before or after commit | After commit (memtable = committed state only) |
| SK update sync or async | Synchronous (just memtable insert, µs) |
| SK crash recovery | Replay shared WAL from SK's lastAppliedSeqNum to HWM |
| Out-of-order commits | Impossible (sequential WAL + ordered TCP stream + contiguous HWM) |
| Replication transport | Raw TCP + sendfile() zero-copy |
| PK uniqueness mechanism | Sharded PKLocker + bloom filter + pending_set |
| OCC under replication | pending_set prevents stale OCC checks on leader |

## Open Questions

| Question | Status |
|----------|--------|
| Watch API detailed design | Deferred to Phase 5 (tractable after Phase 4) |
| Leader election protocol details | TBD in Phase 3 |
| ReadIndex vs Leader Lease | TBD in Phase 4 (implement one or both) |

---

*Architecture revised February 2026 to reflect replicated WAL design with ISR replication.*
