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
- `RegisterSchemaVersion` - creates first version of a new type
- `UpdateSchemaVersion` - adds subsequent versions (with compatibility checks)
- `SetCurrentSchemaVersion` - explicitly activates a version
- `GetSchemaVersion` - retrieves a specific version
- `ListSchemaTypes` - lists all types (current version of each)
- `ListSchemaVersions` - lists all versions of a type

### 3. Separate Register vs Update

Clear semantic distinction:
- `RegisterSchemaVersion` - registers a NEW version (v1, v2, v3...). Creates type + resources if type doesn't exist.
- `UpdateSchemaVersion` - modifies an EXISTING version in place (add optional fields, new indexes, etc.)

This allows:
- Creating new versions when needed (Register)
- Evolving existing versions without version churn (Update)

### 4. Client-Side Validation (No Server Validate RPC)

Schema validation is done client-side via linting tools:
- JSON syntax validation via standard JSON linters
- Field type validation via schema definition linter
- Evolution compatibility via fetching `ListSchemaVersions` and checking locally

Benefits:
- Simpler server API (6 RPCs instead of 7)
- GitOps-friendly: lint in CI without server calls
- Validation logic can be shared as a library

### 5. Explicit Version Activation (No Auto-Current)

Neither `RegisterSchemaVersion` nor `UpdateSchemaVersion` auto-set current version.
This enables safe production workflows:
- Register/Update a new version
- Test via integration tests
- Explicitly activate via `SetCurrentSchemaVersion`
- Rollback by setting current to a previous version

### 6. Schema as JSON String (GitOps-friendly)

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

### 7. Nested Field Indexing (Dot Notation)

Secondary indexes support nested fields using dot notation:
- `"user_id"` - top-level field
- `"metadata.region"` - nested field within object
- `"address.city"` - deeply nested field

This enables flexible querying on JSON document structures.

### 8. Backend Resource Provisioning

`RegisterSchemaVersion` provisions backend resources on first version:
- WAL for the new type
- State Machine for snapshot/PK lookups
- LSMs for each secondary index

### 9. Pagination Support

List APIs include pagination for scalability:
- `page_size` - max results per page (default 100, max 1000)
- `page_token` - opaque token for next page
- `next_page_token` - returned in response
- `total_count` - total matching items across all pages

### 10. Field Mask Support

`GetSchemaVersionRequest` includes optional `google.protobuf.FieldMask` for partial responses.

### 11. Evolution Rules Encoded in Proto

`SchemaEvolutionError` and `EvolutionViolation` make evolution rules explicit:
- Primary key cannot change
- Field types cannot change
- New required fields must have defaults

## API Overview

```protobuf
service SchemaService {
  // Register a new version (v1, v2, etc.). Creates type if needed.
  rpc RegisterSchemaVersion(RegisterSchemaVersionRequest) returns (RegisterSchemaVersionResponse);

  // Update an existing version in place (add fields, indexes, etc.).
  rpc UpdateSchemaVersion(UpdateSchemaVersionRequest) returns (UpdateSchemaVersionResponse);

  // Explicitly sets current version. Enables rollback.
  rpc SetCurrentSchemaVersion(SetCurrentSchemaVersionRequest) returns (SetCurrentSchemaVersionResponse);

  // Retrieves a version (defaults to current if version omitted).
  rpc GetSchemaVersion(GetSchemaVersionRequest) returns (GetSchemaVersionResponse);

  // Lists all types (current version of each).
  rpc ListSchemaTypes(ListSchemaTypesRequest) returns (ListSchemaTypesResponse);

  // Lists all versions of a type.
  rpc ListSchemaVersions(ListSchemaVersionsRequest) returns (ListSchemaVersionsResponse);
}
```

## Typical Workflows

### New Type Registration
```
1. Lint schema locally (CI job)
2. RegisterSchemaVersion(schema_json)   # Create type + v1
3. SetCurrentSchemaVersion(type, "v1")  # Activate
```

### Add New Version
```
1. Lint schema locally (CI job)
2. RegisterSchemaVersion(schema_json)   # Add v2 to existing type
3. SetCurrentSchemaVersion(type, "v2")  # Activate v2
```

### Update Existing Version (add optional fields, indexes)
```
1. Lint schema locally + check evolution rules (CI job)
2. UpdateSchemaVersion(schema_json)     # Modify v1 in place
# No SetCurrentSchemaVersion needed - v1 is already current
```

### Rollback
```
1. SetCurrentSchemaVersion(type, "v1")   # Back to v1
```

## Future: Resource API Integration

The Resource API (future PR) will allow specifying schema version on writes:
```protobuf
message CreateResourceRequest {
  string type = 1;
  string schema_version = 2;  // Explicit version to validate against
  bytes data = 3;
}
```

This enables gradual migration where some clients write v1, others write v2.

## Reference Code

None for this PR (foundational API definition).

## Memory/Performance Considerations

N/A for proto definitions. Implementation PRs will address:
- Schema registry memory footprint
- Schema lookup performance
- JSON parsing overhead (mitigated by caching parsed schemas)

## Learning Points

1. **Proto3 best practices**:
   - Let buf managed mode handle `go_package`
   - Prefix enums with type name (`FIELD_TYPE_STRING`)
   - Use `google.protobuf.FieldMask` for partial responses

2. **Buf tooling**:
   - `buf lint` catches common proto issues
   - `buf generate` with managed mode handles go_package automatically
   - Remote plugins avoid local protoc installation

3. **gRPC API design**:
   - Consistent naming (SchemaVersion throughout)
   - Separate Register vs Update for clear semantics
   - Client-side validation (simpler server, GitOps-friendly)
   - Explicit state transitions (SetCurrentSchemaVersion)

4. **GitOps-friendly API**:
   - Accept JSON strings for human-authored content
   - Return structured proto messages for programmatic access
   - Client-side linting for pipeline integration

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
| Support nested fields for secondary index | Added dot notation support |
| Mention WAL/LSMs in RegisterSchema comment | Updated comment to mention backend resources |
| Use proto FieldMask | Added field_mask to GetSchemaVersionRequest |
| Add pagination to ListSchemas | Added pagination to ListSchemaTypes |
| Add pagination to ListSchemaVersions | Added pagination fields |
| Schema as JSON instead of proto | Changed to schema_json string input |
| Naming inconsistency (Schema vs Version) | Renamed all RPCs to use "SchemaVersion" consistently |
| Auto-current version is wrong | Added SetCurrentSchemaVersion for explicit activation |
| ListSchemas unclear semantics | Renamed to ListSchemaTypes |
| UpdateSchemaVersion needed | Added for in-place updates to existing versions |
| Register vs Update semantics | Register=new version, Update=modify existing version |
| Server-side validation unnecessary | Removed ValidateSchemaVersion; use client-side linting |

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
