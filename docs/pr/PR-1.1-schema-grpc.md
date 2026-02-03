# PR 1.1: Schema gRPC API

## Summary

Defines the gRPC service for schema management, including protobuf message definitions and service methods.

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

### 2. Schema as JSON String (GitOps-friendly)

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
- Language-agnostic tooling

### 3. Nested Field Indexing (Dot Notation)

Secondary indexes support nested fields using dot notation:
- `"user_id"` - top-level field
- `"metadata.region"` - nested field within object
- `"address.city"` - deeply nested field

This enables flexible querying on JSON document structures.

### 4. Backend Resource Provisioning

RegisterSchema comment explicitly documents that backend resources are created:
- WAL for the new type
- State Machine for snapshot/PK lookups
- LSMs for each secondary index

### 5. Pagination Support

List APIs include pagination for scalability:
- `page_size` - max results per page (default 100, max 1000)
- `page_token` - opaque token for next page
- `next_page_token` - returned in response
- `total_count` - total matching items across all pages

### 6. Field Mask Support

GetSchemaRequest includes optional `google.protobuf.FieldMask` for partial responses:
- Reduces payload size for clients that only need specific fields
- Standard proto pattern for selective retrieval

### 7. Field Types

Supported types chosen for:
- **Indexability**: STRING, INT64, DOUBLE, BOOL, TIMESTAMP can be secondary index keys
- **Flexibility**: OBJECT (with nested indexing via dot notation), ARRAY for complex data
- **Timestamps**: RFC 3339 string format for human readability

### 8. Evolution Rules Encoded in Proto

`SchemaEvolutionError` and `EvolutionViolation` make evolution rules explicit:
- Clients get structured errors, not just strings
- Documentation is in the API contract

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
   - Separate Get (single) from List (multiple)
   - Use request/response wrappers for extensibility
   - Include metadata for server-assigned fields
   - Always include pagination for list APIs

4. **GitOps-friendly API**:
   - Accept JSON strings for human-authored content
   - Return structured proto messages for programmatic access

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
| Use proto FieldMask | Added field_mask to GetSchemaRequest |
| Add pagination to ListSchemas | Added page_size, page_token, next_page_token, total_count |
| Add pagination to ListSchemaVersions | Added page_size, page_token, next_page_token, total_count |
| Schema as JSON instead of proto | Changed to schema_json string input |

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
