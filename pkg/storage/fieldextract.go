package storage

import (
	"encoding/json"
	"fmt"
	"strings"
)

// extractField extracts a field value from JSON data using a dot-separated path.
// Supports nested fields like "metadata.region".
//
// Returns the field value as a string suitable for SK key construction.
// For non-string types:
//   - numbers: JSON representation (e.g., "42", "3.14")
//   - booleans: "true" or "false"
//   - null/missing: returns ("", false)
//
// Returns ("", false) if the path does not exist or the value is null.
func extractField(data []byte, fieldPath string) (string, bool) {
	if len(data) == 0 {
		return "", false
	}

	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return "", false
	}

	parts := strings.Split(fieldPath, ".")
	return traversePath(obj, parts)
}

// traversePath walks a map by the given path segments and returns the leaf value as a string.
func traversePath(obj map[string]any, parts []string) (string, bool) {
	if len(parts) == 0 {
		return "", false
	}

	val, ok := obj[parts[0]]
	if !ok || val == nil {
		return "", false
	}

	// Intermediate path: descend into nested object.
	if len(parts) > 1 {
		nested, ok := val.(map[string]any)
		if !ok {
			return "", false
		}
		return traversePath(nested, parts[1:])
	}

	// Leaf: convert to string.
	return valueToString(val)
}

// valueToString converts a JSON value to a string representation.
func valueToString(val any) (string, bool) {
	switch v := val.(type) {
	case string:
		return v, true
	case float64:
		// JSON numbers are float64. Use fmt for consistent representation.
		return fmt.Sprintf("%g", v), true
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}
