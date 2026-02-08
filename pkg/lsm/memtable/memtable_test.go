package memtable

import (
	"bytes"
	"fmt"
	"testing"
)

const defaultArenaSize = 1 << 20 // 1MB

func TestInternalKey_RoundTrip(t *testing.T) {
	tests := []struct {
		key    string
		seqNum uint64
		kind   Kind
	}{
		{"hello", 1, KindSet},
		{"world", 100, KindDelete},
		{"", 0, KindSet},
		{"key", 1<<56 - 1, KindSet}, // large seqNum
	}

	for _, tc := range tests {
		ikey := MakeInternalKey([]byte(tc.key), tc.seqNum, tc.kind)
		gotKey, gotSeq, gotKind := ParseInternalKey(ikey)

		if string(gotKey) != tc.key {
			t.Errorf("key: got %q, want %q", gotKey, tc.key)
		}
		if gotSeq != tc.seqNum {
			t.Errorf("seqNum: got %d, want %d", gotSeq, tc.seqNum)
		}
		if gotKind != tc.kind {
			t.Errorf("kind: got %d, want %d", gotKind, tc.kind)
		}
	}
}

func TestInternalKey_SortOrder(t *testing.T) {
	// Same user key, different seqNums: higher seqNum should sort first.
	k1 := MakeInternalKey([]byte("key"), 10, KindSet)
	k2 := MakeInternalKey([]byte("key"), 5, KindSet)
	k3 := MakeInternalKey([]byte("key"), 1, KindSet)

	if bytes.Compare(k1, k2) >= 0 {
		t.Error("seqNum 10 should sort before seqNum 5")
	}
	if bytes.Compare(k2, k3) >= 0 {
		t.Error("seqNum 5 should sort before seqNum 1")
	}

	// Different user keys: sorted lexicographically.
	ka := MakeInternalKey([]byte("aaa"), 1, KindSet)
	kb := MakeInternalKey([]byte("bbb"), 1, KindSet)
	if bytes.Compare(ka, kb) >= 0 {
		t.Error("aaa should sort before bbb")
	}

	// Same user key and seqNum, different kind: KindSet (1) > KindDelete (0)
	// So KindSet sorts before KindDelete (lower trailer value).
	ks := MakeInternalKey([]byte("key"), 5, KindSet)
	kd := MakeInternalKey([]byte("key"), 5, KindDelete)
	if bytes.Compare(ks, kd) >= 0 {
		t.Error("KindSet should sort before KindDelete at same seqNum")
	}
}

func TestInternalKey_UserKey(t *testing.T) {
	ikey := MakeInternalKey([]byte("hello"), 42, KindSet)
	uk := UserKey(ikey)
	if string(uk) != "hello" {
		t.Errorf("UserKey = %q, want %q", uk, "hello")
	}
}

func TestMemtable_SetAndGet(t *testing.T) {
	m := New(defaultArenaSize)

	if err := m.Set([]byte("key1"), []byte("val1"), 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := m.Set([]byte("key2"), []byte("val2"), 2); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, ok := m.Get([]byte("key1"), 10)
	if !ok || string(val) != "val1" {
		t.Errorf("Get(key1) = %q, %v; want val1, true", val, ok)
	}

	val, ok = m.Get([]byte("key2"), 10)
	if !ok || string(val) != "val2" {
		t.Errorf("Get(key2) = %q, %v; want val2, true", val, ok)
	}

	// Key not found.
	_, ok = m.Get([]byte("missing"), 10)
	if ok {
		t.Error("Get(missing): should not be found")
	}
}

func TestMemtable_MVCC(t *testing.T) {
	m := New(defaultArenaSize)

	// Write two versions of the same key.
	m.Set([]byte("key"), []byte("v1"), 1)
	m.Set([]byte("key"), []byte("v2"), 5)

	// At seqNum 10: should see v2 (latest).
	val, ok := m.Get([]byte("key"), 10)
	if !ok || string(val) != "v2" {
		t.Errorf("Get@10 = %q, %v; want v2, true", val, ok)
	}

	// At seqNum 3: should see v1 (v2 not yet visible).
	val, ok = m.Get([]byte("key"), 3)
	if !ok || string(val) != "v1" {
		t.Errorf("Get@3 = %q, %v; want v1, true", val, ok)
	}

	// At seqNum 0: nothing visible.
	_, ok = m.Get([]byte("key"), 0)
	if ok {
		t.Error("Get@0: should not be visible")
	}
}

func TestMemtable_Delete(t *testing.T) {
	m := New(defaultArenaSize)

	m.Set([]byte("key"), []byte("val"), 1)
	m.Delete([]byte("key"), 5)

	// At seqNum 10: tombstone visible, should return not found.
	_, ok := m.Get([]byte("key"), 10)
	if ok {
		t.Error("Get after delete: should not be found")
	}

	// At seqNum 3: tombstone not yet visible, should see the value.
	val, ok := m.Get([]byte("key"), 3)
	if !ok || string(val) != "val" {
		t.Errorf("Get@3 after delete = %q, %v; want val, true", val, ok)
	}
}

func TestMemtable_DeleteThenSet(t *testing.T) {
	m := New(defaultArenaSize)

	m.Set([]byte("key"), []byte("v1"), 1)
	m.Delete([]byte("key"), 5)
	m.Set([]byte("key"), []byte("v2"), 10)

	// At seqNum 15: should see v2.
	val, ok := m.Get([]byte("key"), 15)
	if !ok || string(val) != "v2" {
		t.Errorf("Get@15 = %q, %v; want v2, true", val, ok)
	}

	// At seqNum 7: should see tombstone (not found).
	_, ok = m.Get([]byte("key"), 7)
	if ok {
		t.Error("Get@7: should see tombstone")
	}
}

func TestMemtable_SeqNumTracking(t *testing.T) {
	m := New(defaultArenaSize)

	m.Set([]byte("a"), []byte("1"), 5)
	m.Set([]byte("b"), []byte("2"), 3)
	m.Set([]byte("c"), []byte("3"), 10)

	if m.MinSeqNum() != 3 {
		t.Errorf("MinSeqNum = %d, want 3", m.MinSeqNum())
	}
	if m.MaxSeqNum() != 10 {
		t.Errorf("MaxSeqNum = %d, want 10", m.MaxSeqNum())
	}
}

func TestMemtable_IsFull(t *testing.T) {
	m := New(4096) // Tiny arena.

	if m.IsFull() {
		t.Error("should not be full initially")
	}

	// Fill it up.
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%06d", i)
		err := m.Set([]byte(k), []byte(k), uint64(i+1))
		if err != nil {
			break
		}
		if m.IsFull() {
			break
		}
	}
}

func TestMemtable_Iterator(t *testing.T) {
	m := New(defaultArenaSize)

	// Insert multiple keys with different versions.
	m.Set([]byte("a"), []byte("a1"), 1)
	m.Set([]byte("a"), []byte("a2"), 5)
	m.Set([]byte("b"), []byte("b1"), 2)
	m.Delete([]byte("c"), 3)
	m.Set([]byte("d"), []byte("d1"), 4)

	it := m.NewIterator()
	type entry struct {
		userKey string
		seqNum  uint64
		kind    Kind
	}

	var entries []entry
	for it.First(); it.Valid(); it.Next() {
		entries = append(entries, entry{
			userKey: string(it.UserKey()),
			seqNum:  it.SeqNum(),
			kind:    it.Kind(),
		})
	}

	// Expected order: a@5, a@1, b@2, c@3(del), d@4
	expected := []entry{
		{"a", 5, KindSet},
		{"a", 1, KindSet},
		{"b", 2, KindSet},
		{"c", 3, KindDelete},
		{"d", 4, KindSet},
	}

	if len(entries) != len(expected) {
		t.Fatalf("got %d entries, want %d", len(entries), len(expected))
	}
	for i, e := range entries {
		if e != expected[i] {
			t.Errorf("entry %d: got %+v, want %+v", i, e, expected[i])
		}
	}
}

func TestMemtable_IteratorSeekGE(t *testing.T) {
	m := New(defaultArenaSize)

	m.Set([]byte("aa"), []byte("1"), 1)
	m.Set([]byte("cc"), []byte("2"), 2)
	m.Set([]byte("ee"), []byte("3"), 3)

	it := m.NewIterator()

	// Seek to internal key for "cc" at max seqNum — should find cc.
	target := MakeInternalKey([]byte("cc"), ^uint64(0)>>8, KindSet)
	if !it.SeekGE(target) {
		t.Fatal("SeekGE(cc): not found")
	}
	if string(it.UserKey()) != "cc" {
		t.Errorf("SeekGE(cc): userKey = %q, want cc", it.UserKey())
	}
}

func TestMemtable_RefCounting(t *testing.T) {
	m := New(defaultArenaSize)
	m.Ref()
	m.Ref()

	if m.Unref() {
		t.Error("Unref should not hit zero yet")
	}
	if m.Unref() {
		t.Error("Unref should not hit zero yet")
	}
	if !m.Unref() {
		t.Error("Unref should hit zero")
	}
}
