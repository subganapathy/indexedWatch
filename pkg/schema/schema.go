package schema

import (
	"fmt"

	schemav1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/schema/v1"
)

// Schema is the internal representation of a schema version.
// It wraps the protobuf SchemaVersion with derived fields for fast lookups.
type Schema struct {
	Type             string
	Version          string
	PrimaryKey       string
	SecondaryIndexes []string
	Fields           map[string]Field
}

// Field is the internal representation of a field definition.
type Field struct {
	Type         schemav1.FieldType
	Required     bool
	DefaultValue string
	Description  string
}

// FromProto converts a protobuf SchemaVersion to the internal Schema type.
func FromProto(sv *schemav1.SchemaVersion) *Schema {
	if sv == nil {
		return nil
	}

	fields := make(map[string]Field, len(sv.GetFields()))
	for name, fd := range sv.GetFields() {
		fields[name] = Field{
			Type:         fd.GetType(),
			Required:     fd.GetRequired(),
			DefaultValue: fd.GetDefaultValue(),
			Description:  fd.GetDescription(),
		}
	}

	indexes := make([]string, len(sv.GetSecondaryIndexes()))
	copy(indexes, sv.GetSecondaryIndexes())

	return &Schema{
		Type:             sv.GetType(),
		Version:          sv.GetVersion(),
		PrimaryKey:       sv.GetPrimaryKey(),
		SecondaryIndexes: indexes,
		Fields:           fields,
	}
}

// ToProto converts the internal Schema type to a protobuf SchemaVersion.
func (s *Schema) ToProto() *schemav1.SchemaVersion {
	if s == nil {
		return nil
	}

	fields := make(map[string]*schemav1.FieldDefinition, len(s.Fields))
	for name, f := range s.Fields {
		fields[name] = &schemav1.FieldDefinition{
			Type:         f.Type,
			Required:     f.Required,
			DefaultValue: f.DefaultValue,
			Description:  f.Description,
		}
	}

	indexes := make([]string, len(s.SecondaryIndexes))
	copy(indexes, s.SecondaryIndexes)

	return &schemav1.SchemaVersion{
		Type:             s.Type,
		Version:          s.Version,
		PrimaryKey:       s.PrimaryKey,
		SecondaryIndexes: indexes,
		Fields:           fields,
	}
}

// Validate checks that the schema definition is well-formed.
func (s *Schema) Validate() error {
	if s.Type == "" {
		return fmt.Errorf("%w: type is required", ErrInvalidSchema)
	}
	if s.Version == "" {
		return fmt.Errorf("%w: version is required", ErrInvalidSchema)
	}
	if s.PrimaryKey == "" {
		return fmt.Errorf("%w: primary key is required", ErrInvalidSchema)
	}
	if len(s.Fields) == 0 {
		return fmt.Errorf("%w: at least one field is required", ErrInvalidSchema)
	}

	// Primary key must reference a defined field.
	if _, ok := s.Fields[s.PrimaryKey]; !ok {
		return fmt.Errorf("%w: primary key %q not found in fields", ErrInvalidSchema, s.PrimaryKey)
	}

	// Secondary indexes must reference defined fields (top-level part for dot notation).
	for _, idx := range s.SecondaryIndexes {
		topLevel := idx
		for i, c := range idx {
			if c == '.' {
				topLevel = idx[:i]
				break
			}
		}
		if _, ok := s.Fields[topLevel]; !ok {
			return fmt.Errorf("%w: secondary index %q references undefined field %q", ErrInvalidSchema, idx, topLevel)
		}
	}

	// All fields must have a valid type.
	for name, f := range s.Fields {
		if f.Type == schemav1.FieldType_FIELD_TYPE_UNSPECIFIED {
			return fmt.Errorf("%w: field %q has unspecified type", ErrInvalidSchema, name)
		}
	}

	return nil
}
