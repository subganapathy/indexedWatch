package schema

import (
	"fmt"

	schemav1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/schema/v1"
	"google.golang.org/protobuf/proto"
)

// walEntry wraps the generated SchemaWALEntry with helpers for constructing
// each operation type and marshaling to/from the opaque bytes the WAL stores.

// marshalRegister creates a REGISTER WAL entry and marshals it to bytes.
func marshalRegister(s *Schema) ([]byte, error) {
	entry := &schemav1.SchemaWALEntry{
		Operation: schemav1.SchemaOperation_SCHEMA_OPERATION_REGISTER,
		Schema:    s.ToProto(),
	}
	return proto.Marshal(entry)
}

// marshalUpdate creates an UPDATE WAL entry and marshals it to bytes.
func marshalUpdate(s *Schema) ([]byte, error) {
	entry := &schemav1.SchemaWALEntry{
		Operation: schemav1.SchemaOperation_SCHEMA_OPERATION_UPDATE,
		Schema:    s.ToProto(),
	}
	return proto.Marshal(entry)
}

// marshalSetCurrent creates a SET_CURRENT WAL entry and marshals it to bytes.
func marshalSetCurrent(typeName, version string) ([]byte, error) {
	entry := &schemav1.SchemaWALEntry{
		Operation: schemav1.SchemaOperation_SCHEMA_OPERATION_SET_CURRENT,
		Type:      typeName,
		Version:   version,
	}
	return proto.Marshal(entry)
}

// unmarshalEntry deserializes a WAL entry from opaque bytes.
func unmarshalEntry(data []byte) (*schemav1.SchemaWALEntry, error) {
	entry := &schemav1.SchemaWALEntry{}
	if err := proto.Unmarshal(data, entry); err != nil {
		return nil, fmt.Errorf("schema: failed to unmarshal WAL entry: %w", err)
	}
	return entry, nil
}
