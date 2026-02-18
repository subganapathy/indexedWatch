package arena

import (
	"sync"
	"testing"
)

func TestArena_Basic(t *testing.T) {
	a := New(make([]byte, 1024))

	// First alloc should succeed.
	off, err := a.Alloc(64, 4)
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	if off == 0 {
		t.Fatal("offset 0 is reserved")
	}

	// Write and read back.
	data := []byte("hello, arena")
	a.PutBytes(off, data)
	got := a.GetBytes(off, uint32(len(data)))
	if string(got) != string(data) {
		t.Errorf("GetBytes = %q, want %q", got, data)
	}
}

func TestArena_NilOffset(t *testing.T) {
	a := New(make([]byte, 1024))
	if got := a.GetBytes(0, 10); got != nil {
		t.Errorf("GetBytes(0) = %v, want nil", got)
	}
}

func TestArena_Alignment(t *testing.T) {
	a := New(make([]byte, 4096))

	// Allocate an odd-sized chunk, then verify next allocation is aligned.
	off1, err := a.Alloc(7, 4)
	if err != nil {
		t.Fatalf("Alloc 7: %v", err)
	}
	if off1%4 != 0 {
		t.Errorf("off1 = %d, not 4-byte aligned", off1)
	}

	off2, err := a.Alloc(16, 8)
	if err != nil {
		t.Fatalf("Alloc 16: %v", err)
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
	a := New(make([]byte, 128))

	// Fill it up.
	_, err := a.Alloc(120, 4)
	if err != nil {
		t.Fatalf("Alloc 120: %v", err)
	}

	// Next alloc should fail.
	_, err = a.Alloc(64, 4)
	if err != ErrFull {
		t.Errorf("Alloc on full arena: got %v, want ErrFull", err)
	}
}

func TestArena_SizeAndCapacity(t *testing.T) {
	a := New(make([]byte, 4096))

	if a.Capacity() != 4096 {
		t.Errorf("Capacity = %d, want 4096", a.Capacity())
	}
	// Initial size is 1 (reserved byte).
	if a.Size() != 1 {
		t.Errorf("initial Size = %d, want 1", a.Size())
	}

	a.Alloc(100, 4)
	if a.Size() <= 1 {
		t.Error("Size should have increased after allocation")
	}
}

func TestArena_Reset(t *testing.T) {
	a := New(make([]byte, 1024))
	a.Alloc(512, 4)
	a.Reset()
	if a.Size() != 1 {
		t.Errorf("after Reset: Size = %d, want 1", a.Size())
	}
}

func TestArena_ConcurrentAlloc(t *testing.T) {
	a := New(make([]byte, 1<<20)) // 1MB

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
				off, err := a.Alloc(allocSize, 4)
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
	a := New(make([]byte, 1<<30)) // 1GB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Alloc(128, 4)
	}
}
