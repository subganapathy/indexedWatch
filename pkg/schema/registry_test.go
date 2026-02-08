package schema

import (
	"errors"
	"path/filepath"
	"testing"

	schemav1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/schema/v1"
)

// testSchema returns a valid schema for testing.
func testSchema(typeName, version, pk string) *Schema {
	return &Schema{
		Type:       typeName,
		Version:    version,
		PrimaryKey: pk,
		Fields: map[string]Field{
			pk: {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
		},
	}
}

// testSchemaFull returns a schema with multiple fields and secondary indexes.
func testSchemaFull(typeName, version string) *Schema {
	return &Schema{
		Type:             typeName,
		Version:          version,
		PrimaryKey:       "id",
		SecondaryIndexes: []string{"user_id", "region"},
		Fields: map[string]Field{
			"id":      {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"user_id": {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"region":  {Type: schemav1.FieldType_FIELD_TYPE_STRING},
			"data":    {Type: schemav1.FieldType_FIELD_TYPE_BYTES},
		},
	}
}

func openTestRegistry(t *testing.T) *Registry {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "schema-wal")
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchemaFull("events", "v1")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := r.Get("events", "v1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Type != "events" || got.Version != "v1" {
		t.Errorf("Get = %s/%s, want events/v1", got.Type, got.Version)
	}
	if got.PrimaryKey != "id" {
		t.Errorf("PrimaryKey = %q, want %q", got.PrimaryKey, "id")
	}
	if len(got.SecondaryIndexes) != 2 {
		t.Errorf("SecondaryIndexes len = %d, want 2", len(got.SecondaryIndexes))
	}
	if len(got.Fields) != 4 {
		t.Errorf("Fields len = %d, want 4", len(got.Fields))
	}
}

func TestRegistry_RegisterDuplicate(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "v1", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := r.Register(s)
	if !errors.Is(err, ErrVersionExists) {
		t.Errorf("Register duplicate: got %v, want ErrVersionExists", err)
	}
}

func TestRegistry_MultipleVersions(t *testing.T) {
	r := openTestRegistry(t)

	s1 := testSchema("events", "v1", "id")
	s2 := &Schema{
		Type:       "events",
		Version:    "v2",
		PrimaryKey: "id",
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"name": {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}

	if err := r.Register(s1); err != nil {
		t.Fatalf("Register v1: %v", err)
	}
	if err := r.Register(s2); err != nil {
		t.Fatalf("Register v2: %v", err)
	}

	versions, err := r.ListVersions("events")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 2 {
		t.Errorf("ListVersions len = %d, want 2", len(versions))
	}
}

func TestRegistry_SetCurrentAndGetCurrent(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "v1", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// No current set yet.
	_, err := r.GetCurrent("events")
	if !errors.Is(err, ErrNoCurrentVersion) {
		t.Errorf("GetCurrent before set: got %v, want ErrNoCurrentVersion", err)
	}

	if err := r.SetCurrent("events", "v1"); err != nil {
		t.Fatalf("SetCurrent: %v", err)
	}

	got, err := r.GetCurrent("events")
	if err != nil {
		t.Fatalf("GetCurrent: %v", err)
	}
	if got.Version != "v1" {
		t.Errorf("GetCurrent version = %q, want v1", got.Version)
	}
}

func TestRegistry_SetCurrentNonexistent(t *testing.T) {
	r := openTestRegistry(t)

	err := r.SetCurrent("events", "v1")
	if !errors.Is(err, ErrTypeNotFound) {
		t.Errorf("SetCurrent nonexistent type: got %v, want ErrTypeNotFound", err)
	}

	s := testSchema("events", "v1", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err = r.SetCurrent("events", "v99")
	if !errors.Is(err, ErrVersionNotFound) {
		t.Errorf("SetCurrent nonexistent version: got %v, want ErrVersionNotFound", err)
	}
}

func TestRegistry_Update(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "v1", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Add an optional field — valid evolution.
	updated := &Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "id",
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"name": {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}
	if err := r.Update(updated); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := r.Get("events", "v1")
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if _, ok := got.Fields["name"]; !ok {
		t.Error("Field 'name' not found after update")
	}
}

func TestRegistry_UpdateNonexistent(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "v1", "id")
	err := r.Update(s)
	if !errors.Is(err, ErrTypeNotFound) {
		t.Errorf("Update nonexistent type: got %v, want ErrTypeNotFound", err)
	}

	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	s2 := testSchema("events", "v99", "id")
	err = r.Update(s2)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Errorf("Update nonexistent version: got %v, want ErrVersionNotFound", err)
	}
}

func TestRegistry_GetNonexistent(t *testing.T) {
	r := openTestRegistry(t)

	_, err := r.Get("nope", "v1")
	if !errors.Is(err, ErrTypeNotFound) {
		t.Errorf("Get nonexistent type: got %v, want ErrTypeNotFound", err)
	}

	s := testSchema("events", "v1", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	_, err = r.Get("events", "v99")
	if !errors.Is(err, ErrVersionNotFound) {
		t.Errorf("Get nonexistent version: got %v, want ErrVersionNotFound", err)
	}
}

func TestRegistry_ListTypes(t *testing.T) {
	r := openTestRegistry(t)

	if err := r.Register(testSchema("events", "v1", "id")); err != nil {
		t.Fatalf("Register events: %v", err)
	}
	if err := r.Register(testSchema("users", "v1", "id")); err != nil {
		t.Fatalf("Register users: %v", err)
	}

	types := r.ListTypes()
	if len(types) != 2 {
		t.Errorf("ListTypes len = %d, want 2", len(types))
	}
}

// --- Evolution validation tests ---

func TestEvolution_PrimaryKeyChanged(t *testing.T) {
	r := openTestRegistry(t)

	s := &Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "id",
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"uuid": {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated := &Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "uuid", // changed!
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"uuid": {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}
	err := r.Update(updated)
	if !errors.Is(err, ErrPrimaryKeyChanged) {
		t.Errorf("Update with PK change: got %v, want ErrPrimaryKeyChanged", err)
	}
}

func TestEvolution_FieldTypeChanged(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchemaFull("events", "v1")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated := &Schema{
		Type:             "events",
		Version:          "v1",
		PrimaryKey:       "id",
		SecondaryIndexes: []string{"user_id", "region"},
		Fields: map[string]Field{
			"id":      {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"user_id": {Type: schemav1.FieldType_FIELD_TYPE_INT64, Required: true}, // changed!
			"region":  {Type: schemav1.FieldType_FIELD_TYPE_STRING},
			"data":    {Type: schemav1.FieldType_FIELD_TYPE_BYTES},
		},
	}
	err := r.Update(updated)
	if !errors.Is(err, ErrFieldTypeChanged) {
		t.Errorf("Update with field type change: got %v, want ErrFieldTypeChanged", err)
	}
}

func TestEvolution_RequiredFieldNoDefault(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "v1", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated := &Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "id",
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"name": {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true}, // required, no default!
		},
	}
	err := r.Update(updated)
	if !errors.Is(err, ErrRequiredFieldNoDefault) {
		t.Errorf("Update with required-no-default: got %v, want ErrRequiredFieldNoDefault", err)
	}
}

func TestEvolution_RequiredFieldWithDefault(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "v1", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated := &Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "id",
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"name": {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true, DefaultValue: `"unknown"`},
		},
	}
	if err := r.Update(updated); err != nil {
		t.Errorf("Update with required+default: %v", err)
	}
}

func TestEvolution_RemoveField(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchemaFull("events", "v1")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Remove the "data" field — should be allowed.
	updated := &Schema{
		Type:             "events",
		Version:          "v1",
		PrimaryKey:       "id",
		SecondaryIndexes: []string{"user_id", "region"},
		Fields: map[string]Field{
			"id":      {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"user_id": {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"region":  {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}
	if err := r.Update(updated); err != nil {
		t.Errorf("Update removing field: %v", err)
	}
}

func TestEvolution_AddSecondaryIndex(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "v1", "id")
	s.Fields["region"] = Field{Type: schemav1.FieldType_FIELD_TYPE_STRING}
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated := &Schema{
		Type:             "events",
		Version:          "v1",
		PrimaryKey:       "id",
		SecondaryIndexes: []string{"region"},
		Fields: map[string]Field{
			"id":     {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"region": {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}
	if err := r.Update(updated); err != nil {
		t.Errorf("Update adding secondary index: %v", err)
	}
}

// --- WAL recovery tests ---

func TestRegistry_RecoveryFromWAL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "schema-wal")

	// Phase 1: Create registry, register schemas, close.
	r1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r1: %v", err)
	}

	s1 := testSchemaFull("events", "v1")
	if err := r1.Register(s1); err != nil {
		t.Fatalf("Register events/v1: %v", err)
	}

	s2 := testSchema("users", "v1", "id")
	if err := r1.Register(s2); err != nil {
		t.Fatalf("Register users/v1: %v", err)
	}

	if err := r1.SetCurrent("events", "v1"); err != nil {
		t.Fatalf("SetCurrent events: %v", err)
	}

	if err := r1.Close(); err != nil {
		t.Fatalf("Close r1: %v", err)
	}

	// Phase 2: Reopen — state must be recovered from WAL.
	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r2: %v", err)
	}
	defer r2.Close()

	types := r2.ListTypes()
	if len(types) != 2 {
		t.Errorf("After recovery: ListTypes len = %d, want 2", len(types))
	}

	got, err := r2.Get("events", "v1")
	if err != nil {
		t.Fatalf("After recovery: Get events/v1: %v", err)
	}
	if got.PrimaryKey != "id" {
		t.Errorf("After recovery: PrimaryKey = %q, want %q", got.PrimaryKey, "id")
	}
	if len(got.SecondaryIndexes) != 2 {
		t.Errorf("After recovery: SecondaryIndexes len = %d, want 2", len(got.SecondaryIndexes))
	}

	current, err := r2.GetCurrent("events")
	if err != nil {
		t.Fatalf("After recovery: GetCurrent events: %v", err)
	}
	if current.Version != "v1" {
		t.Errorf("After recovery: current = %q, want v1", current.Version)
	}
}

func TestRegistry_RecoveryWithUpdates(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "schema-wal")

	// Phase 1: Register, update, close.
	r1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r1: %v", err)
	}

	s := testSchema("events", "v1", "id")
	if err := r1.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated := &Schema{
		Type:       "events",
		Version:    "v1",
		PrimaryKey: "id",
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"name": {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}
	if err := r1.Update(updated); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := r1.Close(); err != nil {
		t.Fatalf("Close r1: %v", err)
	}

	// Phase 2: Reopen — should see updated schema.
	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r2: %v", err)
	}
	defer r2.Close()

	got, err := r2.Get("events", "v1")
	if err != nil {
		t.Fatalf("After recovery: Get: %v", err)
	}
	if _, ok := got.Fields["name"]; !ok {
		t.Error("After recovery: field 'name' not found")
	}
	if len(got.Fields) != 2 {
		t.Errorf("After recovery: Fields len = %d, want 2", len(got.Fields))
	}
}

func TestRegistry_RecoverySetCurrentSwitch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "schema-wal")

	r1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r1: %v", err)
	}

	if err := r1.Register(testSchema("events", "v1", "id")); err != nil {
		t.Fatalf("Register v1: %v", err)
	}
	s2 := &Schema{
		Type:       "events",
		Version:    "v2",
		PrimaryKey: "id",
		Fields: map[string]Field{
			"id":   {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
			"name": {Type: schemav1.FieldType_FIELD_TYPE_STRING},
		},
	}
	if err := r1.Register(s2); err != nil {
		t.Fatalf("Register v2: %v", err)
	}

	if err := r1.SetCurrent("events", "v1"); err != nil {
		t.Fatalf("SetCurrent v1: %v", err)
	}
	if err := r1.SetCurrent("events", "v2"); err != nil {
		t.Fatalf("SetCurrent v2: %v", err)
	}

	if err := r1.Close(); err != nil {
		t.Fatalf("Close r1: %v", err)
	}

	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r2: %v", err)
	}
	defer r2.Close()

	current, err := r2.GetCurrent("events")
	if err != nil {
		t.Fatalf("After recovery: GetCurrent: %v", err)
	}
	if current.Version != "v2" {
		t.Errorf("After recovery: current = %q, want v2", current.Version)
	}
}

// --- Validation tests ---

func TestSchema_Validate(t *testing.T) {
	tests := []struct {
		name    string
		schema  *Schema
		wantErr bool
	}{
		{
			name:    "valid",
			schema:  testSchema("events", "v1", "id"),
			wantErr: false,
		},
		{
			name: "missing type",
			schema: &Schema{
				Version:    "v1",
				PrimaryKey: "id",
				Fields:     map[string]Field{"id": {Type: schemav1.FieldType_FIELD_TYPE_STRING}},
			},
			wantErr: true,
		},
		{
			name: "missing version",
			schema: &Schema{
				Type:       "events",
				PrimaryKey: "id",
				Fields:     map[string]Field{"id": {Type: schemav1.FieldType_FIELD_TYPE_STRING}},
			},
			wantErr: true,
		},
		{
			name: "missing primary key",
			schema: &Schema{
				Type:    "events",
				Version: "v1",
				Fields:  map[string]Field{"id": {Type: schemav1.FieldType_FIELD_TYPE_STRING}},
			},
			wantErr: true,
		},
		{
			name: "no fields",
			schema: &Schema{
				Type:       "events",
				Version:    "v1",
				PrimaryKey: "id",
			},
			wantErr: true,
		},
		{
			name: "pk not in fields",
			schema: &Schema{
				Type:       "events",
				Version:    "v1",
				PrimaryKey: "uuid",
				Fields:     map[string]Field{"id": {Type: schemav1.FieldType_FIELD_TYPE_STRING}},
			},
			wantErr: true,
		},
		{
			name: "unspecified field type",
			schema: &Schema{
				Type:       "events",
				Version:    "v1",
				PrimaryKey: "id",
				Fields:     map[string]Field{"id": {Type: schemav1.FieldType_FIELD_TYPE_UNSPECIFIED}},
			},
			wantErr: true,
		},
		{
			name: "secondary index undefined field",
			schema: &Schema{
				Type:             "events",
				Version:          "v1",
				PrimaryKey:       "id",
				SecondaryIndexes: []string{"nope"},
				Fields:           map[string]Field{"id": {Type: schemav1.FieldType_FIELD_TYPE_STRING}},
			},
			wantErr: true,
		},
		{
			name: "secondary index dot notation valid",
			schema: &Schema{
				Type:             "events",
				Version:          "v1",
				PrimaryKey:       "id",
				SecondaryIndexes: []string{"metadata.region"},
				Fields: map[string]Field{
					"id":       {Type: schemav1.FieldType_FIELD_TYPE_STRING, Required: true},
					"metadata": {Type: schemav1.FieldType_FIELD_TYPE_OBJECT},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.schema.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

// --- Closed registry tests ---

func TestRegistry_Closed(t *testing.T) {
	r := openTestRegistry(t)
	r.Close()

	s := testSchema("events", "v1", "id")

	if err := r.Register(s); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("Register on closed: got %v, want ErrRegistryClosed", err)
	}
	if err := r.Update(s); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("Update on closed: got %v, want ErrRegistryClosed", err)
	}
	if err := r.SetCurrent("events", "v1"); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("SetCurrent on closed: got %v, want ErrRegistryClosed", err)
	}
	if _, err := r.Get("events", "v1"); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("Get on closed: got %v, want ErrRegistryClosed", err)
	}
	if _, err := r.GetCurrent("events"); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("GetCurrent on closed: got %v, want ErrRegistryClosed", err)
	}
}
