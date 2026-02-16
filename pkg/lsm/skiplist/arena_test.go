package skiplist

import (
	"sync"
	"testing"
)

func TestArena_Basic(t *testing.T) {
	a := newArena(make([]byte, 1024))

	// First alloc should succeed.
	off, err := a.alloc(64, 4, 0)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if off == 0 {
		t.Fatal("offset 0 is reserved")
	}

	// Write and read back.
	data := []byte("hello, arena")
	a.putBytes(off, data)
	got := a.getBytes(off, uint32(len(data)))
	if string(got) != string(data) {
		t.Errorf("getBytes = %q, want %q", got, data)
	}
}

func TestArena_NilOffset(t *testing.T) {
	a := newArena(make([]byte, 1024))
	if got := a.getBytes(0, 10); got != nil {
		t.Errorf("getBytes(0) = %v, want nil", got)
	}
}

func TestArena_NilPointer(t *testing.T) {
	a := newArena(make([]byte, 1024))
	if got := a.getPointer(0); got != nil {
		t.Errorf("getPointer(0) = %v, want nil", got)
	}
	if got := a.getPointerOffset(nil); got != 0 {
		t.Errorf("getPointerOffset(nil) = %d, want 0", got)
	}
}

func TestArena_Alignment(t *testing.T) {
	a := newArena(make([]byte, 4096))

	// Allocate an odd-sized chunk, then verify next allocation is aligned.
	off1, err := a.alloc(7, 4, 0)
	if err != nil {
		t.Fatalf("alloc 7: %v", err)
	}
	if off1%4 != 0 {
		t.Errorf("off1 = %d, not 4-byte aligned", off1)
	}

	off2, err := a.alloc(16, 8, 0)
	if err != nil {
		t.Fatalf("alloc 16: %v", err)
	}
	if off2%8 != 0 {
		t.Errorf("off2 = %d, not 8-byte aligned", off2)
	}

	// Allocations should not overlap.
	if off2 < off1+7 {
		t.Errorf("off2 %d overlaps with off1 %d + size 7", off2, off1)
	}
}

func TestArena_Full(t *testing.T) {
	a := newArena(make([]byte, 128))

	// Fill it up.
	_, err := a.alloc(120, 4, 0)
	if err != nil {
		t.Fatalf("alloc 120: %v", err)
	}

	// Next alloc should fail.
	_, err = a.alloc(64, 4, 0)
	if err != errArenaFull {
		t.Errorf("alloc on full arena: got %v, want errArenaFull", err)
	}
}

func TestArena_FullEarlyExit(t *testing.T) {
	a := newArena(make([]byte, 128))

	// Exhaust the arena.
	a.alloc(120, 4, 0)
	a.alloc(64, 4, 0) // pushes n past len(buf)

	// Subsequent alloc should hit the early-exit check (origSize > len(buf))
	// without doing another atomic Add.
	_, err := a.alloc(1, 1, 0)
	if err != errArenaFull {
		t.Errorf("alloc on exhausted arena: got %v, want errArenaFull", err)
	}
}

func TestArena_Overflow(t *testing.T) {
	// Arena with exactly 128 bytes.
	a := newArena(make([]byte, 128))

	// Allocate 64 bytes with 60 bytes of overflow.
	// The alloc needs: 64 bytes of data + 60 bytes of overflow space after = 124 bytes used.
	// Plus offset 0 is reserved, so we start at 1.
	off, err := a.alloc(64, 4, 60)
	if err != nil {
		t.Fatalf("alloc with overflow: %v", err)
	}
	if off == 0 {
		t.Fatal("offset 0 is reserved")
	}

	// Now try another alloc that would succeed without overflow concern
	// but the previous alloc's overflow doesn't block it — overflow is about
	// the struct cast being within bounds, not about reservation.
	_, err = a.alloc(20, 4, 0)
	if err != nil {
		t.Fatalf("second alloc should succeed: %v", err)
	}
}

func TestArena_OverflowAtEnd(t *testing.T) {
	// Arena that can hold 80 bytes of data (offset 0 reserved).
	a := newArena(make([]byte, 80))

	// Allocate 40 bytes at the beginning (no overflow).
	_, err := a.alloc(40, 4, 0)
	if err != nil {
		t.Fatalf("first alloc: %v", err)
	}

	// Try to allocate 20 bytes with 30 bytes overflow.
	// The data fits, but data + overflow would extend past the buffer.
	_, err = a.alloc(20, 4, 30)
	if err != errArenaFull {
		t.Errorf("alloc with overflow past end: got %v, want errArenaFull", err)
	}
}

func TestArena_SizeAndCapacity(t *testing.T) {
	a := newArena(make([]byte, 4096))

	if a.capacity() != 4096 {
		t.Errorf("capacity = %d, want 4096", a.capacity())
	}
	// Initial size is 1 (reserved byte).
	if a.size() != 1 {
		t.Errorf("initial size = %d, want 1", a.size())
	}

	a.alloc(100, 4, 0)
	if a.size() <= 1 {
		t.Error("size should have increased after allocation")
	}
}

func TestArena_SizeSaturation(t *testing.T) {
	a := newArena(make([]byte, 128))
	a.alloc(120, 4, 0)
	a.alloc(64, 4, 0) // pushes n past len(buf)

	// Size should saturate at capacity, not overflow.
	if a.size() != 128 {
		t.Errorf("size after overflow = %d, want 128", a.size())
	}
}

func TestArena_PointerRoundTrip(t *testing.T) {
	a := newArena(make([]byte, 1024))

	off, err := a.alloc(64, 4, 0)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}

	ptr := a.getPointer(off)
	gotOff := a.getPointerOffset(ptr)
	if gotOff != off {
		t.Errorf("round-trip: getPointerOffset(getPointer(%d)) = %d", off, gotOff)
	}
}

func TestArena_Reset(t *testing.T) {
	a := newArena(make([]byte, 1024))
	a.alloc(512, 4, 0)
	a.reset()
	if a.size() != 1 {
		t.Errorf("after reset: size = %d, want 1", a.size())
	}
}

func TestArena_ConcurrentAlloc(t *testing.T) {
	a := newArena(make([]byte, 1<<20)) // 1MB

	const goroutines = 8
	const allocsPerGoroutine = 1000
	const allocSize = 64

	var wg sync.WaitGroup
	offsets := make([][]uint32, goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		offsets[g] = make([]uint32, 0, allocsPerGoroutine)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < allocsPerGoroutine; i++ {
				off, err := a.alloc(allocSize, 4, 0)
				if err != nil {
					return // Arena full is ok.
				}
				offsets[g] = append(offsets[g], off)
			}
		}()
	}
	wg.Wait()

	// Verify no overlaps: collect all [off, off+allocSize) and check.
	type span struct{ start, end uint32 }
	var spans []span
	for _, group := range offsets {
		for _, off := range group {
			spans = append(spans, span{off, off + allocSize})
		}
	}

	// Sort by start and check for overlaps.
	for i := 0; i < len(spans); i++ {
		for j := i + 1; j < len(spans); j++ {
			if spans[i].start < spans[j].end && spans[j].start < spans[i].end {
				t.Fatalf("overlap between [%d,%d) and [%d,%d)",
					spans[i].start, spans[i].end, spans[j].start, spans[j].end)
			}
		}
	}
}

func BenchmarkArena_Alloc(b *testing.B) {
	a := newArena(make([]byte, 1<<30)) // 1GB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.alloc(128, 4, 0)
	}
}

func BenchmarkArena_AllocParallel(b *testing.B) {
	a := newArena(make([]byte, 1<<30)) // 1GB
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a.alloc(128, 4, 0)
		}
	})
}
