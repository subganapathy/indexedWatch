package schema

import (
	"fmt"
	"strings"

	schemav1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/schema/v1"
)

// Schema is the internal representation of a schema definition.
// It defines what to index for a resource type — not what fields exist.
// Field validation is the API layer's responsibility (like K8s API server).
type Schema struct {
	Type             string   // Resource type, e.g., "pods"
	PrimaryKey       string   // JSON path, e.g., "metadata.name"
	SecondaryIndexes []string // JSON paths, e.g., "spec.nodeName", "metadata.labels.*"
}

// FromProto converts a protobuf SchemaDefinition to the internal Schema type.
func FromProto(sd *schemav1.SchemaDefinition) *Schema {
	if sd == nil {
		return nil
	}

	indexes := make([]string, len(sd.GetSecondaryIndexes()))
	copy(indexes, sd.GetSecondaryIndexes())

	return &Schema{
		Type:             sd.GetType(),
		PrimaryKey:       sd.GetPrimaryKey(),
		SecondaryIndexes: indexes,
	}
}

// ToProto converts the internal Schema type to a protobuf SchemaDefinition.
func (s *Schema) ToProto() *schemav1.SchemaDefinition {
	if s == nil {
		return nil
	}

	indexes := make([]string, len(s.SecondaryIndexes))
	copy(indexes, s.SecondaryIndexes)

	return &schemav1.SchemaDefinition{
		Type:             s.Type,
		PrimaryKey:       s.PrimaryKey,
		SecondaryIndexes: indexes,
	}
}

// Validate checks that the schema definition is well-formed.
//
// Rules:
//   - Type is required
//   - PrimaryKey is required and cannot use wildcard suffix
//   - SecondaryIndexes: each must be non-empty, not equal to PrimaryKey,
//     ".*" only as final suffix, no duplicates
func (s *Schema) Validate() error {
	if s.Type == "" {
		return fmt.Errorf("%w: type is required", ErrInvalidSchema)
	}
	if s.PrimaryKey == "" {
		return fmt.Errorf("%w: primary key is required", ErrInvalidSchema)
	}
	if strings.HasSuffix(s.PrimaryKey, ".*") {
		return fmt.Errorf("%w: primary key cannot use wildcard", ErrInvalidSchema)
	}

	seen := make(map[string]struct{}, len(s.SecondaryIndexes))
	for _, idx := range s.SecondaryIndexes {
		if idx == "" {
			return fmt.Errorf("%w: secondary index path cannot be empty", ErrInvalidSchema)
		}
		if idx == s.PrimaryKey {
			return fmt.Errorf("%w: secondary index %q duplicates primary key", ErrInvalidSchema, idx)
		}
		// Wildcard ".*" is only valid as a suffix.
		if strings.Contains(idx, ".*") && !strings.HasSuffix(idx, ".*") {
			return fmt.Errorf("%w: wildcard .* must be at end of index path %q", ErrInvalidSchema, idx)
		}
		if _, dup := seen[idx]; dup {
			return fmt.Errorf("%w: duplicate secondary index %q", ErrInvalidSchema, idx)
		}
		seen[idx] = struct{}{}
	}

	return nil
}

// HasIndex returns true if the schema has the given secondary index.
func (s *Schema) HasIndex(indexPath string) bool {
	for _, idx := range s.SecondaryIndexes {
		if idx == indexPath {
			return true
		}
	}
	return false
}
