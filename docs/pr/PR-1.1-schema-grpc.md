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

### 2. Schema as First-Class Citizen

Schema is stored and versioned separately from data because:
- Schema changes are rare, data changes are frequent
- Schema WAL can grow forever (simple lifecycle)
- Enables schema validation before writes

### 3. Field Types

Supported types chosen for:
- **Indexability**: STRING, INT64, DOUBLE, BOOL can be secondary index keys
- **Flexibility**: OBJECT, ARRAY for nested data (not indexable)
- **Timestamps**: TIMESTAMP as string (RFC 3339) for human readability

### 4. Evolution Rules Encoded in Proto

`SchemaEvolutionError` and `EvolutionViolation` make evolution rules explicit:
- Clients get structured errors, not just strings
- Documentation is in the API contract

## Reference Code

None for this PR (foundational API definition).

## Memory/Performance Considerations

N/A for proto definitions. Implementation PRs will address:
- Schema registry memory footprint
- Schema lookup performance

## Learning Points

1. **Proto3 best practices**:
   - Use `option go_package` for generated code location
   - Prefix enums with type name (`FIELD_TYPE_STRING`)
   - Use `oneof` sparingly (not needed here)

2. **Buf tooling**:
   - `buf lint` catches common proto issues
   - `buf generate` with managed mode handles go_package automatically
   - Remote plugins avoid local protoc installation

3. **gRPC service design**:
   - Separate Get (single) from List (multiple)
   - Use request/response wrappers for extensibility
   - Include metadata for server-assigned fields

## Testing

```bash
# Lint proto files
make proto-lint

# Generate code
make proto

# Build to verify compilation
go build ./...
```

## Files Changed

```
indexedWatch/
├── buf.gen.yaml          # Code generation config
├── buf.yaml              # Buf module config
├── cmd/
│   └── indexedwatch/
│       └── main.go       # Placeholder entry point
├── gen/
│   └── go/
│       └── indexedwatch/
│           └── schema/
│               └── v1/
│                   ├── schema.pb.go       # Generated protobuf
│                   └── schema_grpc.pb.go  # Generated gRPC
├── go.mod
├── go.sum
├── Makefile
├── pkg/
│   └── schema/           # (empty, for future implementation)
└── proto/
    └── indexedwatch/
        └── schema/
            └── v1/
                └── schema.proto  # Schema service definition
```

## Next Steps

- PR 1.2: Schema WAL implementation
- PR 1.3: Materialized registry
