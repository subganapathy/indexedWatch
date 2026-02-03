# PR 1.1: Schema gRPC API

## Summary

Defines the gRPC service for schema version management, including protobuf message definitions and service methods.

## What's Included

- `proto/indexedwatch/schema/v1/schema.proto` - Schema service definition
- `buf.yaml` + `buf.gen.yaml` - Buf configuration for linting and code generation
- `Makefile` - Build and proto generation targets
- `gen/go/` - Generated Go code (protobuf + gRPC stubs)
- Basic project structure (`cmd/indexedwatch/`, `pkg/`)

## Design Decisions

### 1. Package Structure: `indexedwatch.schema.v1`

Following API versioning best practices:
- Package includes version (`v1`) for future evolution
- Allows breaking changes in `v2` without affecting existing clients
- Matches buf.build conventions

### 2. Consistent "SchemaVersion" Naming

All RPCs use consistent "SchemaVersion" terminology:
- `RegisterSchemaVersion` - creates a new version (creates type if new)
- `SetCurrentSchemaVersion` - explicitly activates a version
- `GetSchemaVersion` - retrieves a specific version
- `ListSchemaTypes` - lists all types (current version of each)
- `ListSchemaVersions` - lists all versions of a type

This avoids confusion between "Schema" and "Version" concepts.

### 3. Explicit Version Activation (No Auto-Current)

`RegisterSchemaVersion` does NOT automatically set the new version as current.
This enables safe production workflows:
- Register a new version
- Test/validate in staging
- Explicitly activate via `SetCurrentSchemaVersion`
- Rollback by setting current to a previous version

### 4. Schema as JSON String (GitOps-friendly)

Clients submit schemas as JSON strings rather than proto messages:
```json
{
  "type": "events",
  "version": "v1",
  "primaryKey": "id",
  "secondaryIndexes": ["user_id", "metadata.region"],
  "fields": {
    "id": {"type": "string", "required": true},
    "user_id": {"type": "string", "required": true},
    "metadata": {"type": "object"}
  }
}
```

Benefits:
- Schemas can be version-controlled as JSON files
- CI/CD pipelines can lint, diff, and apply schemas
- No proto compilation required for schema authors
- Type and version are in the JSON - no redundant request fields

### 5. Nested Field Indexing (Dot Notation)

Secondary indexes support nested fields using dot notation:
- `"user_id"` - top-level field
- `"metadata.region"` - nested field within object
- `"address.city"` - deeply nested field

This enables flexible querying on JSON document structures.

### 6. Backend Resource Provisioning

`RegisterSchemaVersion` comment documents that backend resources are created on first version:
- WAL for the new type
- State Machine for snapshot/PK lookups
- LSMs for each secondary index

### 7. Pagination Support

List APIs include pagination for scalability:
- `page_size` - max results per page (default 100, max 1000)
- `page_token` - opaque token for next page
- `next_page_token` - returned in response
- `total_count` - total matching items across all pages

### 8. Field Mask Support

`GetSchemaVersionRequest` includes optional `google.protobuf.FieldMask` for partial responses:
- Reduces payload size for clients that only need specific fields
- Standard proto pattern for selective retrieval

### 9. Field Types

Supported types chosen for:
- **Indexability**: STRING, INT64, DOUBLE, BOOL, TIMESTAMP can be secondary index keys
- **Flexibility**: OBJECT (with nested indexing via dot notation), ARRAY for complex data
- **Timestamps**: RFC 3339 string format for human readability

### 10. Evolution Rules Encoded in Proto

`SchemaEvolutionError` and `EvolutionViolation` make evolution rules explicit:
- Clients get structured errors, not just strings
- Documentation is in the API contract

## API Overview

```protobuf
service SchemaService {
  // Creates a schema version. Creates type if new.
  // Does NOT auto-set as current.
  rpc RegisterSchemaVersion(RegisterSchemaVersionRequest) returns (RegisterSchemaVersionResponse);

  // Explicitly sets the current version. Enables rollback.
  rpc SetCurrentSchemaVersion(SetCurrentSchemaVersionRequest) returns (SetCurrentSchemaVersionResponse);

  // Retrieves a version (defaults to current if version omitted).
  rpc GetSchemaVersion(GetSchemaVersionRequest) returns (GetSchemaVersionResponse);

  // Lists all types (current version of each).
  rpc ListSchemaTypes(ListSchemaTypesRequest) returns (ListSchemaTypesResponse);

  // Lists all versions of a type.
  rpc ListSchemaVersions(ListSchemaVersionsRequest) returns (ListSchemaVersionsResponse);
}
```

## Reference Code

None for this PR (foundational API definition).

## Memory/Performance Considerations

N/A for proto definitions. Implementation PRs will address:
- Schema registry memory footprint
- Schema lookup performance
- JSON parsing overhead (mitigated by caching parsed schemas)

## Learning Points

1. **Proto3 best practices**:
   - Let buf managed mode handle `go_package` (removed explicit option)
   - Prefix enums with type name (`FIELD_TYPE_STRING`)
   - Use `google.protobuf.FieldMask` for partial responses

2. **Buf tooling**:
   - `buf lint` catches common proto issues
   - `buf generate` with managed mode handles go_package automatically
   - Remote plugins avoid local protoc installation

3. **gRPC API design**:
   - Consistent naming (SchemaVersion throughout)
   - Explicit state transitions (SetCurrentSchemaVersion)
   - Use request/response wrappers for extensibility
   - Include metadata for server-assigned fields
   - Always include pagination for list APIs

4. **GitOps-friendly API**:
   - Accept JSON strings for human-authored content
   - Return structured proto messages for programmatic access
   - Parse type/version from JSON to avoid redundancy

## Testing

```bash
# Lint proto files
make proto-lint

# Generate code
make proto

# Build to verify compilation
go build ./...
```

## Review Feedback Addressed

| Feedback | Resolution |
|----------|------------|
| go_package mismatch with managed mode | Removed explicit option, let buf handle it |
| Support nested fields for secondary index | Added dot notation support, documented in comments |
| Mention WAL/LSMs in RegisterSchema comment | Updated comment to mention backend resources |
| Use proto FieldMask | Added field_mask to GetSchemaVersionRequest |
| Add pagination to ListSchemas | Added pagination to ListSchemaTypes |
| Add pagination to ListSchemaVersions | Added pagination fields |
| Schema as JSON instead of proto | Changed to schema_json string input |
| Naming inconsistency (Schema vs Version) | Renamed all RPCs to use "SchemaVersion" consistently |
| Type redundant in UpdateSchema | Removed UpdateSchema; type is in JSON |
| Auto-current version is wrong | Added SetCurrentSchemaVersion for explicit activation |
| ListSchemas unclear semantics | Renamed to ListSchemaTypes (lists types, not versions) |

## Files Changed

```
indexedWatch/
├── buf.gen.yaml
├── buf.yaml
├── cmd/
│   └── indexedwatch/
│       └── main.go
├── gen/
│   └── go/
│       └── indexedwatch/
│           └── schema/
│               └── v1/
│                   ├── schema.pb.go
│                   └── schema_grpc.pb.go
├── go.mod
├── go.sum
├── Makefile
├── pkg/
│   └── schema/
└── proto/
    └── indexedwatch/
        └── schema/
            └── v1/
                └── schema.proto
```

## Next Steps

- PR 1.2: Schema WAL implementation
- PR 1.3: Materialized registry (JSON parsing and validation)
