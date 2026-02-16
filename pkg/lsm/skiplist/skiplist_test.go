package skiplist

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

const testArenaSize = 1 << 20 // 1MB

func newTestSkiplist(size int) *Skiplist {
	return New(make([]byte, size))
}

// makeTrailer is a test helper that builds a trailer with InternalKeyKindSet.
func makeTrailer(seqNum uint64) Trailer {
	return MakeTrailer(seqNum, InternalKeyKindSet)
}

func TestSkiplist_BasicInsertGet(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	s.Add([]byte("key1"), makeTrailer(1), []byte("val1"))
	s.Add([]byte("key2"), makeTrailer(2), []byte("val2"))
	s.Add([]byte("key3"), makeTrailer(3), []byte("val3"))

	// SeekGE for each key and verify.
	it := s.NewIterator()
	for _, tc := range []struct {
		key, want string
	}{
		{"key1", "val1"},
		{"key2", "val2"},
		{"key3", "val3"},
	} {
		if !it.SeekGE([]byte(tc.key)) {
			t.Errorf("SeekGE(%q): not found", tc.key)
			continue
		}
		if string(it.Key()) != tc.key {
			t.Errorf("SeekGE(%q): key = %q", tc.key, it.Key())
		}
		if string(it.Value()) != tc.want {
			t.Errorf("SeekGE(%q): value = %q, want %q", tc.key, it.Value(), tc.want)
		}
	}

	// Key not found — past end.
	if it.SeekGE([]byte("zzz")) {
		t.Error("SeekGE(zzz): should not be found")
	}
}

func TestSkiplist_DuplicateInternalKey(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	// Same user key + same trailer = exact duplicate.
	if err := s.Add([]byte("dup"), makeTrailer(1), []byte("v1")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	err := s.Add([]byte("dup"), makeTrailer(1), []byte("v2"))
	if err != ErrRecordExists {
		t.Errorf("Add duplicate: got %v, want ErrRecordExists", err)
	}
}

func TestSkiplist_MVCCVersions(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	// Insert same userKey with different seqNums.
	s.Add([]byte("key"), makeTrailer(1), []byte("v1"))
	s.Add([]byte("key"), makeTrailer(3), []byte("v3"))
	s.Add([]byte("key"), makeTrailer(2), []byte("v2"))

	// Iterate — should see all three in order: (key, seq=3), (key, seq=2), (key, seq=1)
	// because trailer is descending (newer first).
	it := s.NewIterator()
	it.First()

	expected := []struct {
		seqNum uint64
		value  string
	}{
		{3, "v3"},
		{2, "v2"},
		{1, "v1"},
	}

	for i, exp := range expected {
		if !it.Valid() {
			t.Fatalf("element %d: iterator invalid", i)
		}
		if string(it.Key()) != "key" {
			t.Errorf("element %d: key = %q, want %q", i, it.Key(), "key")
		}
		if it.Trailer().SeqNum() != exp.seqNum {
			t.Errorf("element %d: seqNum = %d, want %d", i, it.Trailer().SeqNum(), exp.seqNum)
		}
		if string(it.Value()) != exp.value {
			t.Errorf("element %d: value = %q, want %q", i, it.Value(), exp.value)
		}
		it.Next()
	}
	if it.Valid() {
		t.Error("expected end of iteration")
	}
}

func TestSkiplist_Ordering(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	// Insert in reverse order.
	keys := []string{"delta", "charlie", "bravo", "alpha"}
	for i, k := range keys {
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	// Iterate — should be sorted by user key.
	it := s.NewIterator()
	expected := []string{"alpha", "bravo", "charlie", "delta"}
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if i >= len(expected) {
			t.Fatal("too many elements")
		}
		if string(it.Key()) != expected[i] {
			t.Errorf("element %d: got %q, want %q", i, it.Key(), expected[i])
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("iterated %d elements, want %d", i, len(expected))
	}
}

func TestSkiplist_EmptyValue(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	s.Add([]byte("k"), makeTrailer(1), nil)

	it := s.NewIterator()
	if !it.First() {
		t.Fatal("First: not found")
	}
	if len(it.Value()) != 0 {
		t.Errorf("Value = %q, want empty", it.Value())
	}
}

func TestIterator_SeekGE(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	keys := []string{"aa", "bb", "cc", "dd", "ee"}
	for i, k := range keys {
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	it := s.NewIterator()

	// Exact match.
	if !it.SeekGE([]byte("cc")) {
		t.Fatal("SeekGE(cc): not found")
	}
	if string(it.Key()) != "cc" {
		t.Errorf("SeekGE(cc): key = %q, want cc", it.Key())
	}

	// Between keys — should land on the next one.
	if !it.SeekGE([]byte("bc")) {
		t.Fatal("SeekGE(bc): not found")
	}
	if string(it.Key()) != "cc" {
		t.Errorf("SeekGE(bc): key = %q, want cc", it.Key())
	}

	// Before all keys.
	if !it.SeekGE([]byte("a")) {
		t.Fatal("SeekGE(a): not found")
	}
	if string(it.Key()) != "aa" {
		t.Errorf("SeekGE(a): key = %q, want aa", it.Key())
	}

	// After all keys.
	if it.SeekGE([]byte("zz")) {
		t.Error("SeekGE(zz): should not be found")
	}
}

func TestIterator_SeekLT(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	keys := []string{"aa", "bb", "cc", "dd", "ee"}
	for i, k := range keys {
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	it := s.NewIterator()

	// Exact key — should land on the previous one.
	if !it.SeekLT([]byte("cc")) {
		t.Fatal("SeekLT(cc): not found")
	}
	if string(it.Key()) != "bb" {
		t.Errorf("SeekLT(cc): key = %q, want bb", it.Key())
	}

	// Between keys.
	if !it.SeekLT([]byte("cd")) {
		t.Fatal("SeekLT(cd): not found")
	}
	if string(it.Key()) != "cc" {
		t.Errorf("SeekLT(cd): key = %q, want cc", it.Key())
	}

	// Before all keys.
	if it.SeekLT([]byte("a")) {
		t.Error("SeekLT(a): should not be found")
	}

	// After all keys.
	if !it.SeekLT([]byte("zz")) {
		t.Fatal("SeekLT(zz): not found")
	}
	if string(it.Key()) != "ee" {
		t.Errorf("SeekLT(zz): key = %q, want ee", it.Key())
	}
}

func TestIterator_Last(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	keys := []string{"aa", "bb", "cc"}
	for i, k := range keys {
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	it := s.NewIterator()
	if !it.Last() {
		t.Fatal("Last: not found")
	}
	if string(it.Key()) != "cc" {
		t.Errorf("Last: key = %q, want cc", it.Key())
	}
}

func TestIterator_Prev(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	keys := []string{"aa", "bb", "cc", "dd"}
	for i, k := range keys {
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	// Walk backwards from Last.
	it := s.NewIterator()
	it.Last()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Prev()
	}
	expected := []string{"dd", "cc", "bb", "aa"}
	if len(got) != len(expected) {
		t.Fatalf("Prev walk: got %v, want %v", got, expected)
	}
	for i, g := range got {
		if g != expected[i] {
			t.Errorf("Prev walk[%d]: got %q, want %q", i, g, expected[i])
		}
	}
}

func TestIterator_NextFromSeek(t *testing.T) {
	s := newTestSkiplist(testArenaSize)

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key%02d", i)
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	it := s.NewIterator()
	it.SeekGE([]byte("key05"))

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if len(got) != 5 {
		t.Fatalf("from key05: got %d elements, want 5", len(got))
	}
	if got[0] != "key05" || got[4] != "key09" {
		t.Errorf("range: %v", got)
	}
}

func TestSkiplist_ManyKeys(t *testing.T) {
	s := newTestSkiplist(4 << 20) // 4MB

	const n = 1000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	// Verify all keys exist via SeekGE.
	it := s.NewIterator()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		if !it.SeekGE([]byte(k)) {
			t.Fatalf("SeekGE(%q): not found", k)
		}
		if string(it.Key()) != k {
			t.Fatalf("SeekGE(%q) = %q", k, it.Key())
		}
	}

	// Verify ordering via iteration.
	count := 0
	var prev []byte
	for it.First(); it.Valid(); it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("out of order: %q >= %q", prev, it.Key())
		}
		prev = append(prev[:0], it.Key()...)
		count++
	}
	if count != n {
		t.Errorf("iterated %d, want %d", count, n)
	}
}

func TestSkiplist_ConcurrentInsert(t *testing.T) {
	s := newTestSkiplist(8 << 20) // 8MB

	const goroutines = 8
	const keysPerGoroutine = 500

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range keysPerGoroutine {
				k := fmt.Sprintf("g%02d-key%06d", g, i)
				seq := uint64(g*keysPerGoroutine + i + 1)
				err := s.Add([]byte(k), makeTrailer(seq), []byte(k))
				if err != nil && err != ErrRecordExists {
					t.Errorf("Add(%q): %v", k, err)
				}
			}
		}()
	}
	wg.Wait()

	// Verify all keys via SeekGE.
	it := s.NewIterator()
	for g := range goroutines {
		for i := range keysPerGoroutine {
			k := fmt.Sprintf("g%02d-key%06d", g, i)
			if !it.SeekGE([]byte(k)) {
				t.Errorf("SeekGE(%q): not found after concurrent insert", k)
			}
		}
	}

	// Verify ordering.
	count := 0
	var prev []byte
	var prevTrailer Trailer
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		tr := it.Trailer()
		if prev != nil {
			cmp := bytes.Compare(prev, key)
			if cmp > 0 {
				t.Fatalf("out of order: %q > %q", prev, key)
			}
			if cmp == 0 && prevTrailer <= tr {
				t.Fatalf("same key %q: trailer %d should be > %d", key, prevTrailer, tr)
			}
		}
		prev = append(prev[:0], key...)
		prevTrailer = tr
		count++
	}
	if count != goroutines*keysPerGoroutine {
		t.Errorf("iterated %d, want %d", count, goroutines*keysPerGoroutine)
	}
}

func TestSkiplist_ConcurrentReadWrite(t *testing.T) {
	s := newTestSkiplist(8 << 20) // 8MB

	// Pre-populate some keys.
	for i := range 100 {
		k := fmt.Sprintf("pre-%06d", i)
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}

	var wg sync.WaitGroup

	// Writers.
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				k := fmt.Sprintf("w%d-%06d", g, i)
				seq := uint64(100 + g*500 + i + 1)
				s.Add([]byte(k), makeTrailer(seq), []byte(k))
			}
		}()
	}

	// Concurrent readers.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				it := s.NewIterator()
				var prev []byte
				for it.First(); it.Valid(); it.Next() {
					if prev != nil && bytes.Compare(prev, it.Key()) > 0 {
						t.Errorf("reader saw out of order: %q > %q", prev, it.Key())
						return
					}
					prev = append(prev[:0], it.Key()...)
				}
			}
		}()
	}

	wg.Wait()
}

func TestSkiplist_Height(t *testing.T) {
	s := newTestSkiplist(testArenaSize)
	if s.Height() != 1 {
		t.Errorf("initial height = %d, want 1", s.Height())
	}

	// Insert enough keys that height should grow beyond 1.
	for i := range 100 {
		k := fmt.Sprintf("k%06d", i)
		s.Add([]byte(k), makeTrailer(uint64(i+1)), []byte(k))
	}
	if s.Height() <= 1 {
		t.Error("height should have grown beyond 1 after 100 inserts")
	}
}

func TestIterator_EmptySkiplist(t *testing.T) {
	s := newTestSkiplist(testArenaSize)
	it := s.NewIterator()

	if it.First() {
		t.Error("First on empty: should be invalid")
	}
	if it.Last() {
		t.Error("Last on empty: should be invalid")
	}
	if it.SeekGE([]byte("anything")) {
		t.Error("SeekGE on empty: should be invalid")
	}
	if it.SeekLT([]byte("anything")) {
		t.Error("SeekLT on empty: should be invalid")
	}
}

func TestTrailer(t *testing.T) {
	tr := MakeTrailer(42, InternalKeyKindDelete)
	if tr.SeqNum() != 42 {
		t.Errorf("SeqNum = %d, want 42", tr.SeqNum())
	}
	if tr.Kind() != InternalKeyKindDelete {
		t.Errorf("Kind = %d, want %d", tr.Kind(), InternalKeyKindDelete)
	}

	// Higher seqNum = higher trailer value.
	tr1 := MakeTrailer(1, InternalKeyKindSet)
	tr2 := MakeTrailer(2, InternalKeyKindSet)
	if tr2 <= tr1 {
		t.Error("trailer with higher seqNum should be larger")
	}
}

// --- Benchmarks ---

func BenchmarkSkiplist_Insert(b *testing.B) {
	s := New(make([]byte, 256<<20)) // 256MB
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%012d", i))
	}

	b.ResetTimer()
	for i := range b.N {
		s.Add(keys[i], makeTrailer(uint64(i+1)), keys[i])
	}
}

func BenchmarkSkiplist_SeekGE(b *testing.B) {
	s := New(make([]byte, 64<<20)) // 64MB
	const n = 100000
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = []byte(fmt.Sprintf("key-%012d", i))
		s.Add(keys[i], makeTrailer(uint64(i+1)), keys[i])
	}

	it := s.NewIterator()
	b.ResetTimer()
	for i := range b.N {
		it.SeekGE(keys[i%n])
	}
}

func BenchmarkSkiplist_InsertRandom(b *testing.B) {
	s := New(make([]byte, 256<<20)) // 256MB
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%012d", rand.IntN(b.N*10)))
	}

	b.ResetTimer()
	for i := range b.N {
		s.Add(keys[i], makeTrailer(uint64(i+1)), keys[i])
	}
}
