package schema

import (
	"fmt"

	schemav1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/schema/v1"
	"google.golang.org/protobuf/proto"
)

// marshalRegister creates a REGISTER WAL entry and marshals it to bytes.
func marshalRegister(s *Schema) ([]byte, error) {
	entry := &schemav1.SchemaWALEntry{
		Operation: schemav1.SchemaOperation_SCHEMA_OPERATION_REGISTER,
		Schema:    s.ToProto(),
	}
	return proto.Marshal(entry)
}

// marshalAddIndex creates an ADD_INDEX WAL entry and marshals it to bytes.
func marshalAddIndex(typeName, indexPath string) ([]byte, error) {
	entry := &schemav1.SchemaWALEntry{
		Operation: schemav1.SchemaOperation_SCHEMA_OPERATION_ADD_INDEX,
		Type:      typeName,
		IndexPath: indexPath,
	}
	return proto.Marshal(entry)
}

// marshalRemoveIndex creates a REMOVE_INDEX WAL entry and marshals it to bytes.
func marshalRemoveIndex(typeName, indexPath string) ([]byte, error) {
	entry := &schemav1.SchemaWALEntry{
		Operation: schemav1.SchemaOperation_SCHEMA_OPERATION_REMOVE_INDEX,
		Type:      typeName,
		IndexPath: indexPath,
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
