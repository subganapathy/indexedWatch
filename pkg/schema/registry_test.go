package schema

import (
	"errors"
	"path/filepath"
	"sort"
	"testing"
)

// testSchema returns a valid schema for testing.
func testSchema(typeName, pk string, indexes ...string) *Schema {
	return &Schema{
		Type:             typeName,
		PrimaryKey:       pk,
		SecondaryIndexes: indexes,
	}
}

func openTestRegistry(t *testing.T) Registry {
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

	s := testSchema("events", "id", "user_id", "region")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := r.Get("events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Type != "events" {
		t.Errorf("Type = %q, want %q", got.Type, "events")
	}
	if got.PrimaryKey != "id" {
		t.Errorf("PrimaryKey = %q, want %q", got.PrimaryKey, "id")
	}
	if len(got.SecondaryIndexes) != 2 {
		t.Errorf("SecondaryIndexes len = %d, want 2", len(got.SecondaryIndexes))
	}
}

func TestRegistry_RegisterDuplicate(t *testing.T) {
	r := openTestRegistry(t)

	s := testSchema("events", "id")
	if err := r.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := r.Register(testSchema("events", "id"))
	if !errors.Is(err, ErrTypeExists) {
		t.Errorf("Register duplicate: got %v, want ErrTypeExists", err)
	}
}

func TestRegistry_AddIndex(t *testing.T) {
	r := openTestRegistry(t)

	if err := r.Register(testSchema("events", "id")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := r.AddIndex("events", "user_id"); err != nil {
		t.Fatalf("AddIndex: %v", err)
	}

	got, err := r.Get("events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.HasIndex("user_id") {
		t.Error("expected user_id index after AddIndex")
	}
}

func TestRegistry_AddIndexDuplicate(t *testing.T) {
	r := openTestRegistry(t)

	if err := r.Register(testSchema("events", "id", "user_id")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := r.AddIndex("events", "user_id")
	if !errors.Is(err, ErrIndexExists) {
		t.Errorf("AddIndex duplicate: got %v, want ErrIndexExists", err)
	}
}

func TestRegistry_AddIndexNonexistentType(t *testing.T) {
	r := openTestRegistry(t)

	err := r.AddIndex("nope", "user_id")
	if !errors.Is(err, ErrTypeNotFound) {
		t.Errorf("AddIndex nonexistent: got %v, want ErrTypeNotFound", err)
	}
}

func TestRegistry_AddIndexCannotAddPK(t *testing.T) {
	r := openTestRegistry(t)

	if err := r.Register(testSchema("events", "id")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := r.AddIndex("events", "id")
	if !errors.Is(err, ErrInvalidSchema) {
		t.Errorf("AddIndex PK: got %v, want ErrInvalidSchema", err)
	}
}

func TestRegistry_RemoveIndex(t *testing.T) {
	r := openTestRegistry(t)

	if err := r.Register(testSchema("events", "id", "user_id", "region")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := r.RemoveIndex("events", "user_id"); err != nil {
		t.Fatalf("RemoveIndex: %v", err)
	}

	got, err := r.Get("events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.HasIndex("user_id") {
		t.Error("user_id index should be removed")
	}
	if !got.HasIndex("region") {
		t.Error("region index should still exist")
	}
}

func TestRegistry_RemoveIndexNotFound(t *testing.T) {
	r := openTestRegistry(t)

	if err := r.Register(testSchema("events", "id")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := r.RemoveIndex("events", "nope")
	if !errors.Is(err, ErrIndexNotFound) {
		t.Errorf("RemoveIndex not found: got %v, want ErrIndexNotFound", err)
	}
}

func TestRegistry_RemoveIndexNonexistentType(t *testing.T) {
	r := openTestRegistry(t)

	err := r.RemoveIndex("nope", "user_id")
	if !errors.Is(err, ErrTypeNotFound) {
		t.Errorf("RemoveIndex nonexistent: got %v, want ErrTypeNotFound", err)
	}
}

func TestRegistry_GetNonexistent(t *testing.T) {
	r := openTestRegistry(t)

	_, err := r.Get("nope")
	if !errors.Is(err, ErrTypeNotFound) {
		t.Errorf("Get nonexistent: got %v, want ErrTypeNotFound", err)
	}
}

func TestRegistry_ListTypes(t *testing.T) {
	r := openTestRegistry(t)

	if err := r.Register(testSchema("events", "id")); err != nil {
		t.Fatalf("Register events: %v", err)
	}
	if err := r.Register(testSchema("users", "id")); err != nil {
		t.Fatalf("Register users: %v", err)
	}

	types := r.ListTypes()
	sort.Strings(types)
	if len(types) != 2 {
		t.Fatalf("ListTypes len = %d, want 2", len(types))
	}
	if types[0] != "events" || types[1] != "users" {
		t.Errorf("ListTypes = %v, want [events users]", types)
	}
}

// --- WAL recovery tests ---

func TestRegistry_Recovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "schema-wal")

	// Phase 1: Register types, add/remove indexes, close.
	r1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r1: %v", err)
	}

	if err := r1.Register(testSchema("events", "id", "user_id")); err != nil {
		t.Fatalf("Register events: %v", err)
	}
	if err := r1.Register(testSchema("users", "id")); err != nil {
		t.Fatalf("Register users: %v", err)
	}
	if err := r1.AddIndex("events", "region"); err != nil {
		t.Fatalf("AddIndex region: %v", err)
	}
	if err := r1.RemoveIndex("events", "user_id"); err != nil {
		t.Fatalf("RemoveIndex user_id: %v", err)
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

	events, err := r2.Get("events")
	if err != nil {
		t.Fatalf("After recovery: Get events: %v", err)
	}
	if events.PrimaryKey != "id" {
		t.Errorf("After recovery: PrimaryKey = %q, want %q", events.PrimaryKey, "id")
	}
	// user_id was removed, region was added.
	if events.HasIndex("user_id") {
		t.Error("After recovery: user_id should be removed")
	}
	if !events.HasIndex("region") {
		t.Error("After recovery: region should exist")
	}

	users, err := r2.Get("users")
	if err != nil {
		t.Fatalf("After recovery: Get users: %v", err)
	}
	if users.PrimaryKey != "id" {
		t.Errorf("After recovery: users PrimaryKey = %q, want %q", users.PrimaryKey, "id")
	}
}

func TestRegistry_RecoveryWildcardIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "schema-wal")

	r1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r1: %v", err)
	}

	s := testSchema("pods", "metadata.name", "spec.nodeName")
	if err := r1.Register(s); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r1.AddIndex("pods", "metadata.labels.*"); err != nil {
		t.Fatalf("AddIndex wildcard: %v", err)
	}
	if err := r1.Close(); err != nil {
		t.Fatalf("Close r1: %v", err)
	}

	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open r2: %v", err)
	}
	defer r2.Close()

	got, err := r2.Get("pods")
	if err != nil {
		t.Fatalf("After recovery: Get pods: %v", err)
	}
	if !got.HasIndex("spec.nodeName") {
		t.Error("After recovery: spec.nodeName index missing")
	}
	if !got.HasIndex("metadata.labels.*") {
		t.Error("After recovery: metadata.labels.* index missing")
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
			name:    "valid minimal",
			schema:  testSchema("events", "id"),
			wantErr: false,
		},
		{
			name:    "valid with indexes",
			schema:  testSchema("events", "id", "user_id", "region"),
			wantErr: false,
		},
		{
			name:    "valid wildcard index",
			schema:  testSchema("pods", "metadata.name", "metadata.labels.*"),
			wantErr: false,
		},
		{
			name: "missing type",
			schema: &Schema{
				PrimaryKey: "id",
			},
			wantErr: true,
		},
		{
			name: "missing primary key",
			schema: &Schema{
				Type: "events",
			},
			wantErr: true,
		},
		{
			name: "wildcard primary key",
			schema: &Schema{
				Type:       "events",
				PrimaryKey: "metadata.labels.*",
			},
			wantErr: true,
		},
		{
			name: "empty secondary index",
			schema: &Schema{
				Type:             "events",
				PrimaryKey:       "id",
				SecondaryIndexes: []string{""},
			},
			wantErr: true,
		},
		{
			name: "secondary index equals PK",
			schema: &Schema{
				Type:             "events",
				PrimaryKey:       "id",
				SecondaryIndexes: []string{"id"},
			},
			wantErr: true,
		},
		{
			name: "wildcard not at end",
			schema: &Schema{
				Type:             "events",
				PrimaryKey:       "id",
				SecondaryIndexes: []string{"metadata.*.name"},
			},
			wantErr: true,
		},
		{
			name: "duplicate secondary index",
			schema: &Schema{
				Type:             "events",
				PrimaryKey:       "id",
				SecondaryIndexes: []string{"user_id", "user_id"},
			},
			wantErr: true,
		},
		{
			name:    "dot notation index",
			schema:  testSchema("events", "id", "metadata.region"),
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

	if err := r.Register(testSchema("events", "id")); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("Register on closed: got %v, want ErrRegistryClosed", err)
	}
	if err := r.AddIndex("events", "user_id"); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("AddIndex on closed: got %v, want ErrRegistryClosed", err)
	}
	if err := r.RemoveIndex("events", "user_id"); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("RemoveIndex on closed: got %v, want ErrRegistryClosed", err)
	}
	if _, err := r.Get("events"); !errors.Is(err, ErrRegistryClosed) {
		t.Errorf("Get on closed: got %v, want ErrRegistryClosed", err)
	}
}
