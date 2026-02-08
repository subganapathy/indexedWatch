package schema

import "fmt"

// ValidateEvolution checks that updating from prev to next follows the schema
// evolution rules:
//   - Primary key cannot change
//   - Existing field types cannot change
//   - New required fields must have a default value
//
// Fields may be added (optional or required-with-default) or removed freely.
// Secondary indexes may be added freely (triggers re-indexing at a higher layer).
func ValidateEvolution(prev, next *Schema) error {
	// Primary key immutability.
	if prev.PrimaryKey != next.PrimaryKey {
		return fmt.Errorf("%w: was %q, now %q", ErrPrimaryKeyChanged, prev.PrimaryKey, next.PrimaryKey)
	}

	// For every field that existed in prev and still exists in next,
	// the type must not change.
	for name, prevField := range prev.Fields {
		nextField, exists := next.Fields[name]
		if !exists {
			// Field removal is allowed.
			continue
		}
		if prevField.Type != nextField.Type {
			return fmt.Errorf("%w: field %q was %s, now %s",
				ErrFieldTypeChanged, name, prevField.Type, nextField.Type)
		}
	}

	// New fields that are required must have a default value.
	for name, nextField := range next.Fields {
		if _, existed := prev.Fields[name]; existed {
			continue
		}
		// This is a new field.
		if nextField.Required && nextField.DefaultValue == "" {
			return fmt.Errorf("%w: field %q", ErrRequiredFieldNoDefault, name)
		}
	}

	return nil
}
