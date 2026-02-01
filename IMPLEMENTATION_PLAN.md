# IndexedWatch - Implementation Plan

## Project Goals

### Primary Objectives
| Priority | Goal | Measure |
|----------|------|---------|
| 1 | Production-quality code | Acceptable at FAANG/top AI companies |
| 2 | Deep learning: memory minimization | Zero unnecessary allocations in hot paths |
| 3 | Deep learning: management operations | Proper log pruning, LSM compaction |
| 4 | Distributed algorithms | Raft-based replication |

### Learning Approach
- **PR-based workflow**: Claude submits PRs, you review, ask questions, merge
- **Study PRs**: Before implementing WAL/LSM, study relevant etcd/Pebble patterns
- **Teaching focus**: Each PR explains "why", not just "what"

### Reference Codebases
| Component | Reference | What to Learn |
|-----------|-----------|---------------|
| WAL | etcd/wal | Allocation minimization, CRC, pruning, segment files |
| LSM | Pebble | Arena allocator, skiplist, memtable, SSTable, compaction |
| Raft | etcd/raft | Leader election, log replication |

---

## Git Workflow

```
main
  └── feature/phase-1.1-schema-grpc
        └── PR → Review → Squash Merge → main
  └── feature/phase-1.2-schema-wal
        └── PR → Review → Squash Merge → main
  ...
```

- Feature branches off `main`
- Squash merge after review
- Each PR is self-contained and reviewable

---

## PR Structure

Every PR includes:

```markdown
## Summary
What this PR implements

## Design Decisions
Why this approach, alternatives considered

## Reference Code
Links to etcd/Pebble code that inspired this

## Memory/Performance Considerations
How we minimize allocations, optimize hot paths

## Learning Points
Key concepts to discuss during review

## Testing
How to verify correctness
```

---

## Implementation Phases

### Phase 1: Schema Management

**Goal**: Define schemas, persist to WAL, materialize into registry.

| PR | Title | Description | Learning Focus |
|----|-------|-------------|----------------|
| 1.1 | Schema gRPC API | Protobuf definitions for RegisterSchema, UpdateSchema, GetSchema | gRPC patterns, proto3 best practices |
| 1.2 | Schema WAL | Append-only log for schema changes (grows forever) | Basic WAL structure, fsync patterns |
| 1.3 | Materialized Registry | In-memory (type, version) → Schema, rebuilt on startup | State machine pattern, startup recovery |

**Deliverables**:
- `proto/schema.proto`
- `pkg/schema/wal.go`
- `pkg/schema/registry.go`

---

### Phase 2: Resource APIs

**Goal**: Define gRPC APIs for data operations, generate Go stubs.

| PR | Title | Description | Learning Focus |
|----|-------|-------------|----------------|
| 2.1 | Resource gRPC API | Protobuf for Write, Get, List, Watch, Delete | Streaming gRPC, bidirectional streams |
| 2.2 | Go Client Stubs | buf generate, client library structure | Code generation, API ergonomics |

**Deliverables**:
- `proto/resource.proto`
- `proto/watch.proto`
- `gen/go/` (generated code)
- `pkg/client/` (client library)

---

### Phase 3: WAL Layer (etcd-inspired)

**Goal**: Production-quality WAL with memory efficiency and proper pruning.

| PR | Title | Description | Learning Focus |
|----|-------|-------------|----------------|
| 3.0 | **STUDY: etcd WAL patterns** | Document etcd WAL internals: record format, CRC, segment files, allocation patterns | Deep dive before implementation |
| 3.1 | WAL Record Format | Binary encoding, CRC32, length-prefixed records | Wire format, integrity checking |
| 3.2 | WAL Append + Sync | Append with configurable fsync, batch writes | fsync costs, write amplification |
| 3.3 | WAL Index | seqNum → file mapping, efficient lookup | Index design, binary search |
| 3.4 | WAL File Rotation | Segment files, configurable size limits | File management, atomic rotation |
| 3.5 | WAL Pruning | Track consumer progress, delete old files | Lifecycle management, coordination |
| 3.6 | WAL Recovery | Replay from segment files, handle corruption | Crash recovery, tail corruption |
| 3.7 | Memory-Efficient Encoding | Buffer pooling, zero-copy where possible | sync.Pool, arena patterns |

**Deliverables**:
- `docs/STUDY_ETCD_WAL.md`
- `pkg/wal/record.go`
- `pkg/wal/writer.go`
- `pkg/wal/index.go`
- `pkg/wal/pruner.go`
- `pkg/wal/recovery.go`

**etcd code to study**:
- `wal/wal.go` - main WAL implementation
- `wal/encoder.go` - record encoding
- `wal/decoder.go` - record decoding
- `wal/repair.go` - corruption handling

---

### Phase 4: State Machine

**Goal**: In-memory snapshot store with checkpointing and recovery.

| PR | Title | Description | Learning Focus |
|----|-------|-------------|----------------|
| 4.1 | State Machine Core | In-memory map, Apply(entry), Get(pk) | Concurrent access, RWMutex patterns |
| 4.2 | Checkpointing | Periodic snapshot to disk | Serialization, atomic writes |
| 4.3 | Recovery | Load checkpoint + replay WAL | Startup sequence, consistency |

**Deliverables**:
- `pkg/state/machine.go`
- `pkg/state/checkpoint.go`
- `pkg/state/recovery.go`

---

### Phase 5: LSM Layer (Pebble-inspired)

**Goal**: Production-quality LSM for secondary indexes.

| PR | Title | Description | Learning Focus |
|----|-------|-------------|----------------|
| 5.0 | **STUDY: Pebble LSM patterns** | Document Pebble internals: arena, skiplist, memtable, SSTable, compaction | Deep dive before implementation |
| 5.1 | Arena Allocator | Bump-pointer allocation, single buffer, offsets not pointers | Memory efficiency, GC avoidance |
| 5.2 | Skiplist | Lock-free concurrent skiplist on arena | Probabilistic data structures, CAS |
| 5.3 | Memtable | Skiplist wrapper, size tracking, rotation trigger | Memory accounting, lifecycle |
| 5.4 | SSTable Writer | Block-based format, index block, bloom filter | On-disk format, compression |
| 5.5 | SSTable Reader | Block cache, point lookup, range scan | I/O efficiency, caching |
| 5.6 | Compaction | Level-based, merge sorted runs, tombstone cleanup | Write amplification, space reclamation |
| 5.7 | LSM Integration | WAL consumer, memtable flush, query path | End-to-end data flow |

**Deliverables**:
- `docs/STUDY_PEBBLE_LSM.md`
- `pkg/lsm/arena/arena.go`
- `pkg/lsm/skiplist/skiplist.go`
- `pkg/lsm/memtable/memtable.go`
- `pkg/lsm/sstable/writer.go`
- `pkg/lsm/sstable/reader.go`
- `pkg/lsm/compaction/compaction.go`
- `pkg/lsm/lsm.go`

**Pebble code to study**:
- `internal/arenaskl/arena.go` - arena allocator
- `internal/arenaskl/skl.go` - skiplist
- `internal/arenaskl/node.go` - node layout
- `memtable.go` - memtable management
- `sstable/writer.go` - SSTable format
- `compaction.go` - compaction logic

---

### Phase 6: API Implementation

**Goal**: Wire everything together, implement the gRPC services.

| PR | Title | Description | Learning Focus |
|----|-------|-------------|----------------|
| 6.1 | Admission Checkers | SchemaValidator, PKUniquenessChecker, OCCChecker | Plugin architecture, separation of concerns |
| 6.2 | Write Path | Admission → WAL → State Machine → LSM queue | Transaction flow, error handling |
| 6.3 | Get/List Queries | Point lookup, SK scan with re-check | Query execution, consistency |
| 6.4 | Watch API | Snapshot + streaming, reconnection handling | gRPC streaming, backpressure |
| 6.5 | Strong Consistency | Wait for LSM catch-up option | Consistency modes, latency tradeoffs |
| 6.6 | Server Bootstrap | Startup sequence, graceful shutdown | Lifecycle management |

**Deliverables**:
- `pkg/server/admission/`
- `pkg/server/write.go`
- `pkg/server/query.go`
- `pkg/server/watch.go`
- `pkg/server/server.go`
- `cmd/indexedwatch/main.go`

---

### Phase 7: LinkedIn Milestone 🎉

**Goal**: Working system, ready to demo and share.

| PR | Title | Description |
|----|-------|-------------|
| 7.1 | Demo + Docs | README, architecture diagram, demo script |
| 7.2 | Benchmarks | Performance numbers, comparison with baseline |

**Deliverables**:
- `README.md` (polished)
- `docs/DEMO.md`
- `bench/` (benchmark suite)

---

### Phase 8: Replication (Future)

**Goal**: Raft-based replication for durability and read scaling.

| PR | Title | Description | Learning Focus |
|----|-------|-------------|----------------|
| 8.0 | **STUDY: etcd Raft patterns** | Document etcd Raft internals | Deep dive before implementation |
| 8.1 | Raft Integration | Leader election, log replication | Consensus algorithms |
| 8.2 | Replicated WAL | WAL entries replicated via Raft | Linearizability |
| 8.3 | Read Replicas | Follower reads, stale read option | Read scaling, consistency tradeoffs |

**Deliverables**:
- `docs/STUDY_ETCD_RAFT.md`
- `pkg/raft/`
- `pkg/replication/`

---

## Milestone Summary

| Milestone | PRs | Key Learning |
|-----------|-----|--------------|
| Schema Management | 1.1 - 1.3 | gRPC, basic WAL, state machine |
| Resource APIs | 2.1 - 2.2 | API design, code generation |
| WAL Layer | 3.0 - 3.7 | etcd patterns, memory efficiency, pruning |
| State Machine | 4.1 - 4.3 | Checkpointing, recovery |
| LSM Layer | 5.0 - 5.7 | Pebble patterns, arena, skiplist, compaction |
| API Implementation | 6.1 - 6.6 | End-to-end system |
| LinkedIn | 7.1 - 7.2 | Demo, benchmarks |
| Replication | 8.0 - 8.3 | Raft, distributed systems |

---

## Study PR Format

Study PRs (3.0, 5.0, 8.0) have a different structure:

```markdown
## Overview
What we're studying and why

## Code Walkthrough
Key files and their responsibilities

## Memory Patterns
How the reference code minimizes allocations

## Key Algorithms
Important algorithms with explanations

## Patterns to Adopt
What we'll use in our implementation

## Patterns to Skip
What's not relevant for our use case

## Questions for Discussion
Topics to explore during review
```

---

## Getting Started

**First PR**: `feature/phase-1.1-schema-grpc`

Contents:
- `proto/schema.proto` - Schema service definition
- `buf.yaml` + `buf.gen.yaml` - buf configuration
- `Makefile` - proto generation targets
- Basic project structure

Ready to begin when you are.

---

*Plan created: IndexedWatch Implementation*
