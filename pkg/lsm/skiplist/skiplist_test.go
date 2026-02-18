package skiplist

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/subganapathy/indexedwatch/pkg/lsm/arena"
)

const testArenaSize = 1 << 20 // 1MB

func makeArena() *arena.Arena {
	return arena.New(make([]byte, testArenaSize))
}

func TestSkiplist_BasicInsertGet(t *testing.T) {
	s := New(makeArena())

	if err := s.Add([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Add key1: %v", err)
	}
	if err := s.Add([]byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("Add key2: %v", err)
	}
	if err := s.Add([]byte("key3"), []byte("val3")); err != nil {
		t.Fatalf("Add key3: %v", err)
	}

	for _, tc := range []struct {
		key, want string
	}{
		{"key1", "val1"},
		{"key2", "val2"},
		{"key3", "val3"},
	} {
		val, ok := s.Get([]byte(tc.key))
		if !ok {
			t.Errorf("Get(%q): not found", tc.key)
			continue
		}
		if string(val) != tc.want {
			t.Errorf("Get(%q) = %q, want %q", tc.key, val, tc.want)
		}
	}

	// Key not found.
	_, ok := s.Get([]byte("missing"))
	if ok {
		t.Error("Get(missing): should not be found")
	}
}

func TestSkiplist_DuplicateKey(t *testing.T) {
	s := New(makeArena())

	if err := s.Add([]byte("dup"), []byte("v1")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	err := s.Add([]byte("dup"), []byte("v2"))
	if err != ErrRecordExists {
		t.Errorf("Add duplicate: got %v, want ErrRecordExists", err)
	}

	// Original value should be unchanged.
	val, _ := s.Get([]byte("dup"))
	if string(val) != "v1" {
		t.Errorf("Get(dup) = %q, want %q", val, "v1")
	}
}

func TestSkiplist_Ordering(t *testing.T) {
	s := New(makeArena())

	// Insert in reverse order.
	keys := []string{"delta", "charlie", "bravo", "alpha"}
	for _, k := range keys {
		if err := s.Add([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}

	// Iterate — should be sorted.
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
	s := New(makeArena())

	if err := s.Add([]byte("k"), nil); err != nil {
		t.Fatalf("Add with nil value: %v", err)
	}
	val, ok := s.Get([]byte("k"))
	if !ok {
		t.Fatal("Get: not found")
	}
	if len(val) != 0 {
		t.Errorf("Get = %q, want empty", val)
	}
}

func TestIterator_SeekGE(t *testing.T) {
	s := New(makeArena())

	keys := []string{"aa", "bb", "cc", "dd", "ee"}
	for _, k := range keys {
		s.Add([]byte(k), []byte(k))
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

func TestIterator_NextFromSeek(t *testing.T) {
	s := New(makeArena())

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key%02d", i)
		s.Add([]byte(k), []byte(k))
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
	s := New(arena.New(make([]byte, 4<<20))) // 4MB

	const n = 1000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		if err := s.Add([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	// Verify all keys exist.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		val, ok := s.Get([]byte(k))
		if !ok {
			t.Fatalf("Get(%q): not found", k)
		}
		if string(val) != k {
			t.Fatalf("Get(%q) = %q", k, val)
		}
	}

	// Verify ordering via iteration.
	it := s.NewIterator()
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
	a := arena.New(make([]byte, 8<<20)) // 8MB
	s := New(a)

	const goroutines = 8
	const keysPerGoroutine = 500

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range keysPerGoroutine {
				k := fmt.Sprintf("g%02d-key%06d", g, i)
				err := s.Add([]byte(k), []byte(k))
				if err != nil && err != ErrRecordExists {
					t.Errorf("Add(%q): %v", k, err)
				}
			}
		}()
	}
	wg.Wait()

	// Verify all keys.
	for g := range goroutines {
		for i := range keysPerGoroutine {
			k := fmt.Sprintf("g%02d-key%06d", g, i)
			_, ok := s.Get([]byte(k))
			if !ok {
				t.Errorf("Get(%q): not found after concurrent insert", k)
			}
		}
	}

	// Verify ordering.
	it := s.NewIterator()
	count := 0
	var prev []byte
	for it.First(); it.Valid(); it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("out of order: %q >= %q", prev, it.Key())
		}
		prev = append(prev[:0], it.Key()...)
		count++
	}
	if count != goroutines*keysPerGoroutine {
		t.Errorf("iterated %d, want %d", count, goroutines*keysPerGoroutine)
	}
}

func TestSkiplist_ConcurrentReadWrite(t *testing.T) {
	a := arena.New(make([]byte, 8<<20))
	s := New(a)

	// Pre-populate some keys.
	for i := range 100 {
		k := fmt.Sprintf("pre-%06d", i)
		s.Add([]byte(k), []byte(k))
	}

	var wg sync.WaitGroup

	// Writers.
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				k := fmt.Sprintf("w%d-%06d", g, i)
				s.Add([]byte(k), []byte(k))
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
					if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
						t.Errorf("reader saw out of order: %q >= %q", prev, it.Key())
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
	s := New(makeArena())
	if s.Height() != 1 {
		t.Errorf("initial height = %d, want 1", s.Height())
	}

	// Insert enough keys that height should grow beyond 1.
	for i := range 100 {
		k := fmt.Sprintf("k%06d", i)
		s.Add([]byte(k), []byte(k))
	}
	if s.Height() <= 1 {
		t.Error("height should have grown beyond 1 after 100 inserts")
	}
}

func TestIterator_EmptySkiplist(t *testing.T) {
	s := New(makeArena())
	it := s.NewIterator()

	if it.First() {
		t.Error("First on empty: should be invalid")
	}
	if it.SeekGE([]byte("anything")) {
		t.Error("SeekGE on empty: should be invalid")
	}
}

// --- Benchmarks ---

func BenchmarkSkiplist_Insert(b *testing.B) {
	a := arena.New(make([]byte, 256<<20)) // 256MB
	s := New(a)
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%012d", i))
	}

	b.ResetTimer()
	for i := range b.N {
		s.Add(keys[i], keys[i])
	}
}

func BenchmarkSkiplist_Get(b *testing.B) {
	a := arena.New(make([]byte, 64<<20))
	s := New(a)
	const n = 100000
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = []byte(fmt.Sprintf("key-%012d", i))
		s.Add(keys[i], keys[i])
	}

	b.ResetTimer()
	for i := range b.N {
		s.Get(keys[i%n])
	}
}

func BenchmarkSkiplist_InsertRandom(b *testing.B) {
	a := arena.New(make([]byte, 256<<20))
	s := New(a)
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%012d", rand.IntN(b.N*10)))
	}

	b.ResetTimer()
	for i := range b.N {
		s.Add(keys[i], keys[i])
	}
}
