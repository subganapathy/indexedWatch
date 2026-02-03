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
- `ValidateSchemaVersion` - dry-run validation for CI/CD testing
- `SetCurrentSchemaVersion` - explicitly activates a version
- `GetSchemaVersion` - retrieves a specific version
- `ListSchemaTypes` - lists all types (current version of each)
- `ListSchemaVersions` - lists all versions of a type

### 3. Separate Register vs Update

Clear semantic distinction:
- `RegisterSchemaVersion` - first version of a NEW type; provisions backend resources
- `UpdateSchemaVersion` - subsequent versions of EXISTING type; enforces evolution rules

This prevents accidental type creation and makes the intent explicit.

### 4. Dry-Run Validation for CI/CD

`ValidateSchemaVersion` enables testing schemas before registering:
- Validates JSON syntax and field types
- Checks compatibility with existing versions (if type exists)
- Returns parsed schema for inspection
- Indicates whether this would be Register or Update
- No side effects - safe to call in pipelines

### 5. Explicit Version Activation (No Auto-Current)

Neither `RegisterSchemaVersion` nor `UpdateSchemaVersion` auto-set current version.
This enables safe production workflows:
- Register/Update a new version
- Test with `ValidateSchemaVersion` or integration tests
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

### 9. Structured Validation Errors

`ValidateSchemaVersionResponse` returns detailed error information:
- `ValidationError` with type, message, field, and JSON path
- `ValidationErrorType` enum for programmatic handling
- `compatibility_notes` for evolution check results

### 10. Pagination Support

List APIs include pagination for scalability:
- `page_size` - max results per page (default 100, max 1000)
- `page_token` - opaque token for next page
- `next_page_token` - returned in response
- `total_count` - total matching items across all pages

### 11. Field Mask Support

`GetSchemaVersionRequest` includes optional `google.protobuf.FieldMask` for partial responses.

### 12. Evolution Rules Encoded in Proto

`SchemaEvolutionError` and `EvolutionViolation` make evolution rules explicit:
- Primary key cannot change
- Field types cannot change
- New required fields must have defaults

## API Overview

```protobuf
service SchemaService {
  // First version of a new type. Provisions backend resources.
  rpc RegisterSchemaVersion(RegisterSchemaVersionRequest) returns (RegisterSchemaVersionResponse);

  // Subsequent versions. Enforces evolution rules.
  rpc UpdateSchemaVersion(UpdateSchemaVersionRequest) returns (UpdateSchemaVersionResponse);

  // Dry-run validation. No side effects.
  rpc ValidateSchemaVersion(ValidateSchemaVersionRequest) returns (ValidateSchemaVersionResponse);

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
1. ValidateSchemaVersion(schema_json)  # Test first
2. RegisterSchemaVersion(schema_json)   # Create type + v1
3. SetCurrentSchemaVersion(type, "v1")  # Activate
```

### Backwards-Compatible Update
```
1. ValidateSchemaVersion(schema_json)    # Test compatibility
2. UpdateSchemaVersion(schema_json)      # Add v2
3. SetCurrentSchemaVersion(type, "v2")   # Activate v2
```

### Rollback
```
1. SetCurrentSchemaVersion(type, "v1")   # Back to v1
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
   - Let buf managed mode handle `go_package`
   - Prefix enums with type name (`FIELD_TYPE_STRING`, `VALIDATION_ERROR_TYPE_*`)
   - Use `google.protobuf.FieldMask` for partial responses

2. **Buf tooling**:
   - `buf lint` catches common proto issues
   - `buf generate` with managed mode handles go_package automatically
   - Remote plugins avoid local protoc installation

3. **gRPC API design**:
   - Consistent naming (SchemaVersion throughout)
   - Separate Register vs Update for clear semantics
   - Dry-run validation endpoint for CI/CD
   - Explicit state transitions (SetCurrentSchemaVersion)
   - Structured error responses for programmatic handling

4. **GitOps-friendly API**:
   - Accept JSON strings for human-authored content
   - Return structured proto messages for programmatic access
   - Validation endpoint for pipeline integration

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
| UpdateSchemaVersion needed | Added for backwards-compatible updates with evolution checks |
| Validation/testing endpoint needed | Added ValidateSchemaVersion for dry-run testing |

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
