package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/subganapathy/indexedwatch/pkg/schema"
)

// testSchemaWithIndexes creates a schema with secondary indexes for testing.
func testSchemaWithIndexes() *schema.Schema {
	return &schema.Schema{
		Type:             "events",
		PrimaryKey:       "id",
		SecondaryIndexes: []string{"user_id", "metadata"},
	}
}

// setupRegistryWithIndexes creates a registry with a schema that has secondary indexes.
func setupRegistryWithIndexes(t *testing.T) schema.Registry {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "schema")
	reg, err := schema.Open(dir)
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	s := testSchemaWithIndexes()
	if err := reg.Register(s); err != nil {
		t.Fatalf("register schema: %v", err)
	}
	t.Cleanup(func() { reg.Close() })
	return reg
}

// openTestTypeStoreWithIndexes opens a TypeStore whose schema has secondary indexes.
func openTestTypeStoreWithIndexes(t *testing.T, reg schema.Registry) *TypeStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "types", "events")
	ts, err := OpenTypeStore(dir, "events", reg, testTypeStoreOpts())
	if err != nil {
		t.Fatalf("open type store: %v", err)
	}
	t.Cleanup(func() { ts.Close() })
	return ts
}

func TestListByField(t *testing.T) {
	reg := setupRegistryWithIndexes(t)
	ts := openTestTypeStoreWithIndexes(t, reg)

	// Insert records with different user_ids.
	ts.Create([]byte("evt-1"), []byte(`{"id":"evt-1","user_id":"alice","name":"login"}`))
	ts.Create([]byte("evt-2"), []byte(`{"id":"evt-2","user_id":"bob","name":"logout"}`))
	ts.Create([]byte("evt-3"), []byte(`{"id":"evt-3","user_id":"alice","name":"purchase"}`))
	ts.Create([]byte("evt-4"), []byte(`{"id":"evt-4","user_id":"charlie","name":"signup"}`))
	ts.Create([]byte("evt-5"), []byte(`{"id":"evt-5","user_id":"alice","name":"view"}`))

	// Query by user_id="alice".
	entries, err := ts.ListByField("user_id", []byte("alice"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries for alice, got %d", len(entries))
	}

	// Verify all are alice's events.
	for _, e := range entries {
		pk := string(e.PrimaryKey)
		if pk != "evt-1" && pk != "evt-3" && pk != "evt-5" {
			t.Fatalf("unexpected PK for alice: %s", pk)
		}
	}

	// Query by user_id="bob".
	entries, err = ts.ListByField("user_id", []byte("bob"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry for bob, got %d", len(entries))
	}
	if string(entries[0].PrimaryKey) != "evt-2" {
		t.Fatalf("expected evt-2, got %s", entries[0].PrimaryKey)
	}

	// Query by nonexistent value.
	entries, err = ts.ListByField("user_id", []byte("nobody"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestListByFieldAfterUpdate(t *testing.T) {
	reg := setupRegistryWithIndexes(t)
	ts := openTestTypeStoreWithIndexes(t, reg)

	// Create record with user_id=alice.
	seq, _ := ts.Create([]byte("evt-1"), []byte(`{"id":"evt-1","user_id":"alice","name":"login"}`))

	// Update user_id from alice to bob.
	ts.Update([]byte("evt-1"), []byte(`{"id":"evt-1","user_id":"bob","name":"login"}`), seq)

	// alice should have 0 results.
	entries, err := ts.ListByField("user_id", []byte("alice"))
	if err != nil {
		t.Fatalf("ListByField alice: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for alice after update, got %d", len(entries))
	}

	// bob should have 1 result.
	entries, err = ts.ListByField("user_id", []byte("bob"))
	if err != nil {
		t.Fatalf("ListByField bob: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry for bob, got %d", len(entries))
	}
}

func TestListByFieldAfterDelete(t *testing.T) {
	reg := setupRegistryWithIndexes(t)
	ts := openTestTypeStoreWithIndexes(t, reg)

	seq, _ := ts.Create([]byte("evt-1"), []byte(`{"id":"evt-1","user_id":"alice","name":"login"}`))
	ts.Create([]byte("evt-2"), []byte(`{"id":"evt-2","user_id":"alice","name":"view"}`))

	// Delete evt-1.
	ts.Delete([]byte("evt-1"), seq)

	// alice should have 1 result (evt-2 only).
	entries, err := ts.ListByField("user_id", []byte("alice"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after delete, got %d", len(entries))
	}
	if string(entries[0].PrimaryKey) != "evt-2" {
		t.Fatalf("expected evt-2, got %s", entries[0].PrimaryKey)
	}
}

func TestListByFieldNoIndex(t *testing.T) {
	reg := setupRegistryWithIndexes(t)
	ts := openTestTypeStoreWithIndexes(t, reg)

	// Query a field that has no secondary index.
	_, err := ts.ListByField("name", []byte("login"))
	if err == nil || err.Error() != "storage: secondary index not found: name" {
		t.Fatalf("expected ErrIndexNotFound, got: %v", err)
	}
}

func TestAddSecondaryIndex(t *testing.T) {
	reg := setupRegistryWithIndexes(t)
	ts := openTestTypeStoreWithIndexes(t, reg)

	// Insert records before adding the "name" index.
	ts.Create([]byte("evt-1"), []byte(`{"id":"evt-1","user_id":"alice","name":"login"}`))
	ts.Create([]byte("evt-2"), []byte(`{"id":"evt-2","user_id":"bob","name":"login"}`))
	ts.Create([]byte("evt-3"), []byte(`{"id":"evt-3","user_id":"alice","name":"purchase"}`))

	// "name" is not an existing secondary index, so ListByField fails.
	_, err := ts.ListByField("name", []byte("login"))
	if err == nil {
		t.Fatal("expected error before adding index")
	}

	// Add secondary index on "name" — triggers re-indexing of existing records.
	if err := ts.AddSecondaryIndex("name"); err != nil {
		t.Fatalf("AddSecondaryIndex: %v", err)
	}

	// Now ListByField should work.
	entries, err := ts.ListByField("name", []byte("login"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries for name=login, got %d", len(entries))
	}

	entries, err = ts.ListByField("name", []byte("purchase"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry for name=purchase, got %d", len(entries))
	}
}

func TestAddSecondaryIndexIdempotent(t *testing.T) {
	reg := setupRegistryWithIndexes(t)
	ts := openTestTypeStoreWithIndexes(t, reg)

	ts.Create([]byte("evt-1"), []byte(`{"id":"evt-1","user_id":"alice","name":"x"}`))

	// user_id is already an index from the schema.
	// Adding it again should be a no-op.
	if err := ts.AddSecondaryIndex("user_id"); err != nil {
		t.Fatalf("AddSecondaryIndex: %v", err)
	}

	entries, _ := ts.ListByField("user_id", []byte("alice"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestSecondaryIndexManyRecords(t *testing.T) {
	reg := setupRegistryWithIndexes(t)
	ts := openTestTypeStoreWithIndexes(t, reg)

	// Insert 100 records, cycling through 5 user_ids.
	users := []string{"alice", "bob", "charlie", "dave", "eve"}
	for i := 0; i < 100; i++ {
		pk := fmt.Sprintf("evt-%04d", i)
		user := users[i%5]
		data := fmt.Sprintf(`{"id":"%s","user_id":"%s","name":"event-%d"}`, pk, user, i)
		if _, err := ts.Create([]byte(pk), []byte(data)); err != nil {
			t.Fatalf("create %s: %v", pk, err)
		}
	}

	// Each user should have 20 events.
	for _, user := range users {
		entries, err := ts.ListByField("user_id", []byte(user))
		if err != nil {
			t.Fatalf("ListByField %s: %v", user, err)
		}
		if len(entries) != 20 {
			t.Fatalf("expected 20 entries for %s, got %d", user, len(entries))
		}
	}
}

func TestSecondaryIndexRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "types", "events")
	reg := setupRegistryWithIndexes(t)
	opts := testTypeStoreOpts()

	// Phase 1: create records with indexed fields.
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ts.Create([]byte("evt-1"), []byte(`{"id":"evt-1","user_id":"alice"}`))
	ts.Create([]byte("evt-2"), []byte(`{"id":"evt-2","user_id":"bob"}`))
	ts.Create([]byte("evt-3"), []byte(`{"id":"evt-3","user_id":"alice"}`))

	ts.Close()

	// Phase 2: reopen — secondary indexes should be re-built from Primary LSM.
	ts2, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	entries, err := ts2.ListByField("user_id", []byte("alice"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries for alice after recovery, got %d", len(entries))
	}

	entries, err = ts2.ListByField("user_id", []byte("bob"))
	if err != nil {
		t.Fatalf("ListByField: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry for bob after recovery, got %d", len(entries))
	}
}

func TestFieldExtraction(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		path      string
		expected  string
		expectOK  bool
	}{
		{
			name:     "simple string",
			data:     `{"user_id":"alice","name":"test"}`,
			path:     "user_id",
			expected: "alice",
			expectOK: true,
		},
		{
			name:     "nested field",
			data:     `{"metadata":{"region":"us-east"}}`,
			path:     "metadata.region",
			expected: "us-east",
			expectOK: true,
		},
		{
			name:     "number field",
			data:     `{"count":42}`,
			path:     "count",
			expected: "42",
			expectOK: true,
		},
		{
			name:     "boolean field",
			data:     `{"active":true}`,
			path:     "active",
			expected: "true",
			expectOK: true,
		},
		{
			name:     "missing field",
			data:     `{"user_id":"alice"}`,
			path:     "nonexistent",
			expected: "",
			expectOK: false,
		},
		{
			name:     "null field",
			data:     `{"user_id":null}`,
			path:     "user_id",
			expected: "",
			expectOK: false,
		},
		{
			name:     "deeply nested",
			data:     `{"a":{"b":{"c":"deep"}}}`,
			path:     "a.b.c",
			expected: "deep",
			expectOK: true,
		},
		{
			name:     "empty data",
			data:     "",
			path:     "user_id",
			expected: "",
			expectOK: false,
		},
		{
			name:     "invalid json",
			data:     `{broken}`,
			path:     "user_id",
			expected: "",
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := extractField([]byte(tt.data), tt.path)
			if ok != tt.expectOK {
				t.Fatalf("ok: expected %v, got %v", tt.expectOK, ok)
			}
			if val != tt.expected {
				t.Fatalf("value: expected %q, got %q", tt.expected, val)
			}
		})
	}
}

func TestSKKeyFormat(t *testing.T) {
	key := skKey([]byte("alice"), []byte("evt-1"))
	expected := []byte("alice\x00evt-1")
	if string(key) != string(expected) {
		t.Fatalf("SK key: expected %q, got %q", expected, key)
	}

	prefix := skPrefix([]byte("alice"))
	if string(prefix) != "alice\x00" {
		t.Fatalf("SK prefix: expected %q, got %q", "alice\x00", prefix)
	}

	// Verify hasPrefix works.
	if !hasPrefix(key, prefix) {
		t.Fatal("key should have prefix")
	}
	if hasPrefix([]byte("bob\x00evt-1"), prefix) {
		t.Fatal("different field value should not match prefix")
	}
}
