package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	storagev1 "github.com/subganapathy/indexedwatch/gen/go/indexedwatch/storage/v1"
	"github.com/subganapathy/indexedwatch/pkg/watch"
)

// opToWatchOp converts a storagev1 operation to a watch.Operation.
func opToWatchOp(op storagev1.StorageOperation) watch.Operation {
	switch op {
	case storagev1.StorageOperation_STORAGE_OPERATION_CREATE:
		return watch.OpCreate
	case storagev1.StorageOperation_STORAGE_OPERATION_UPDATE:
		return watch.OpUpdate
	case storagev1.StorageOperation_STORAGE_OPERATION_DELETE:
		return watch.OpDelete
	default:
		return 0
	}
}

func TestWatchIntegrationLiveEvents(t *testing.T) {
	reg := setupRegistry(t)

	wm := watch.NewManager(watch.ManagerOptions{RingBufferCapacity: 64})
	defer wm.Close()

	opts := testTypeStoreOpts()
	opts.OnWrite = func(e WriteEvent) {
		wm.Notify("events", watch.Event{
			SeqNum:        e.SeqNum,
			Operation:     opToWatchOp(e.Operation),
			PrimaryKey:    e.PrimaryKey,
			Data:          e.Data,

		})
	}

	dir := t.TempDir()
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start watching with snapshot.
	snapFn := func() (uint64, []watch.SnapshotResource, error) {
		seqNum, pks, records, err := ts.SnapshotForWatch()
		if err != nil {
			return 0, nil, err
		}
		resources := make([]watch.SnapshotResource, len(pks))
		for i := range pks {
			resources[i] = watch.SnapshotResource{
				PrimaryKey: pks[i],
				Data:       records[i].Data,
			}
		}
		return seqNum, resources, nil
	}

	respCh, errCh := wm.Watch(ctx, "events", 0, snapFn)

	// Consume snapshot (should be empty — no data yet).
	select {
	case resp := <-respCh:
		if resp.Snapshot == nil {
			t.Fatal("expected snapshot response")
		}
		if len(resp.Snapshot.Resources) != 0 {
			t.Fatalf("expected 0 resources in snapshot, got %d", len(resp.Snapshot.Resources))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for snapshot")
	}

	// Write data — should generate watch events.
	ts.Create([]byte("key-1"), []byte(`{"id":"key-1","name":"a"}`))
	ts.Create([]byte("key-2"), []byte(`{"id":"key-2","name":"b"}`))

	// Collect events.
	var allEvents []watch.Event
	deadline := time.After(2 * time.Second)
	for len(allEvents) < 2 {
		select {
		case resp := <-respCh:
			allEvents = append(allEvents, resp.Events...)
		case <-deadline:
			t.Fatalf("timeout: got only %d events", len(allEvents))
		}
	}

	if allEvents[0].Operation != watch.OpCreate || string(allEvents[0].PrimaryKey) != "key-1" {
		t.Fatalf("event[0]: unexpected %+v", allEvents[0])
	}
	if allEvents[1].Operation != watch.OpCreate || string(allEvents[1].PrimaryKey) != "key-2" {
		t.Fatalf("event[1]: unexpected %+v", allEvents[1])
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("watch error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for watch to exit")
	}
}

func TestWatchIntegrationSnapshotWithData(t *testing.T) {
	reg := setupRegistry(t)

	wm := watch.NewManager(watch.ManagerOptions{RingBufferCapacity: 64})
	defer wm.Close()

	opts := testTypeStoreOpts()
	opts.OnWrite = func(e WriteEvent) {
		wm.Notify("events", watch.Event{
			SeqNum:        e.SeqNum,
			Operation:     opToWatchOp(e.Operation),
			PrimaryKey:    e.PrimaryKey,
			Data:          e.Data,

		})
	}

	dir := t.TempDir()
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ts.Close()

	// Write data before starting the watch.
	for i := 0; i < 5; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf(`{"id":"%s","name":"v%d"}`, pk, i)
		ts.Create([]byte(pk), []byte(data))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	snapFn := func() (uint64, []watch.SnapshotResource, error) {
		seqNum, pks, records, err := ts.SnapshotForWatch()
		if err != nil {
			return 0, nil, err
		}
		resources := make([]watch.SnapshotResource, len(pks))
		for i := range pks {
			resources[i] = watch.SnapshotResource{
				PrimaryKey: pks[i],
				Data:       records[i].Data,
			}
		}
		return seqNum, resources, nil
	}

	respCh, _ := wm.Watch(ctx, "events", 0, snapFn)

	// Snapshot should contain 5 resources.
	select {
	case resp := <-respCh:
		if resp.Snapshot == nil {
			t.Fatal("expected snapshot")
		}
		if len(resp.Snapshot.Resources) != 5 {
			t.Fatalf("expected 5 resources, got %d", len(resp.Snapshot.Resources))
		}
		if resp.Snapshot.SeqNum != 5 {
			t.Fatalf("expected snapshot seqNum 5, got %d", resp.Snapshot.SeqNum)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for snapshot")
	}

	cancel()
}

func TestWatchIntegrationResume(t *testing.T) {
	reg := setupRegistry(t)

	wm := watch.NewManager(watch.ManagerOptions{RingBufferCapacity: 64})
	defer wm.Close()

	opts := testTypeStoreOpts()
	opts.OnWrite = func(e WriteEvent) {
		wm.Notify("events", watch.Event{
			SeqNum:        e.SeqNum,
			Operation:     opToWatchOp(e.Operation),
			PrimaryKey:    e.PrimaryKey,
			Data:          e.Data,

		})
	}

	dir := t.TempDir()
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ts.Close()

	// Write 5 events.
	for i := 0; i < 5; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf(`{"id":"%s","name":"v%d"}`, pk, i)
		ts.Create([]byte(pk), []byte(data))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Resume from seqNum 3 — should get events 3, 4, 5.
	respCh, _ := wm.Watch(ctx, "events", 3, nil)

	select {
	case resp := <-respCh:
		if resp.Snapshot != nil {
			t.Fatal("should not get snapshot on resume")
		}
		if len(resp.Events) != 3 {
			t.Fatalf("expected 3 events, got %d", len(resp.Events))
		}
		if resp.Events[0].SeqNum != 3 {
			t.Fatalf("first event seqNum: expected 3, got %d", resp.Events[0].SeqNum)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for resumed events")
	}

	cancel()
}

func TestWatchIntegrationUpdateAndDelete(t *testing.T) {
	reg := setupRegistry(t)

	wm := watch.NewManager(watch.ManagerOptions{RingBufferCapacity: 64})
	defer wm.Close()

	opts := testTypeStoreOpts()
	opts.OnWrite = func(e WriteEvent) {
		wm.Notify("events", watch.Event{
			SeqNum:        e.SeqNum,
			Operation:     opToWatchOp(e.Operation),
			PrimaryKey:    e.PrimaryKey,
			Data:          e.Data,

		})
	}

	dir := t.TempDir()
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	snapFn := func() (uint64, []watch.SnapshotResource, error) {
		seqNum, pks, records, err := ts.SnapshotForWatch()
		if err != nil {
			return 0, nil, err
		}
		resources := make([]watch.SnapshotResource, len(pks))
		for i := range pks {
			resources[i] = watch.SnapshotResource{
				PrimaryKey: pks[i],
				Data:       records[i].Data,
			}
		}
		return seqNum, resources, nil
	}

	respCh, _ := wm.Watch(ctx, "events", 0, snapFn)

	// Consume empty snapshot.
	select {
	case <-respCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Create → Update → Delete.
	createSeq, _ := ts.Create([]byte("key-x"), []byte(`{"id":"key-x","name":"original"}`))
	ts.Update([]byte("key-x"), []byte(`{"id":"key-x","name":"updated"}`), createSeq)
	ts.Delete([]byte("key-x"), createSeq+1)

	// Collect all 3 events.
	var allEvents []watch.Event
	deadline := time.After(2 * time.Second)
	for len(allEvents) < 3 {
		select {
		case resp := <-respCh:
			allEvents = append(allEvents, resp.Events...)
		case <-deadline:
			t.Fatalf("timeout: got only %d events", len(allEvents))
		}
	}

	if allEvents[0].Operation != watch.OpCreate {
		t.Fatalf("event[0]: expected OpCreate, got %d", allEvents[0].Operation)
	}
	if allEvents[1].Operation != watch.OpUpdate {
		t.Fatalf("event[1]: expected OpUpdate, got %d", allEvents[1].Operation)
	}
	if allEvents[2].Operation != watch.OpDelete {
		t.Fatalf("event[2]: expected OpDelete, got %d", allEvents[2].Operation)
	}
	if allEvents[2].Data != nil {
		t.Fatalf("event[2]: delete should have nil data, got %v", allEvents[2].Data)
	}

	cancel()
}

func TestWatchOldestSeqNumConstraint(t *testing.T) {
	reg := setupRegistry(t)

	wm := watch.NewManager(watch.ManagerOptions{RingBufferCapacity: 4})
	defer wm.Close()

	opts := testTypeStoreOpts()
	opts.OnWrite = func(e WriteEvent) {
		wm.Notify("events", watch.Event{
			SeqNum:        e.SeqNum,
			Operation:     opToWatchOp(e.Operation),
			PrimaryKey:    e.PrimaryKey,
			Data:          e.Data,

		})
	}

	dir := t.TempDir()
	ts, err := OpenTypeStore(dir, "events", reg, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ts.Close()

	// Write 10 events — ring buffer capacity is 4, so oldest should be 7.
	for i := 0; i < 10; i++ {
		pk := fmt.Sprintf("key-%04d", i)
		data := fmt.Sprintf(`{"id":"%s","name":"v%d"}`, pk, i)
		ts.Create([]byte(pk), []byte(data))
	}

	oldest := wm.OldestSeqNum("events")
	if oldest != 7 {
		t.Fatalf("expected oldest seqNum 7, got %d", oldest)
	}

	// Trying to resume from seqNum 1 should fail with ErrExpired.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, errCh := wm.Watch(ctx, "events", 1, nil)

	select {
	case err := <-errCh:
		if err != watch.ErrExpired {
			t.Fatalf("expected ErrExpired, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ErrExpired")
	}
}

func TestSnapshotForWatch(t *testing.T) {
	reg := setupRegistry(t)
	ts := openTestTypeStore(t, reg)

	// Write some data.
	ts.Create([]byte("key-1"), []byte(`{"id":"key-1","name":"a"}`))
	ts.Create([]byte("key-2"), []byte(`{"id":"key-2","name":"b"}`))
	seq3, _ := ts.Create([]byte("key-3"), []byte(`{"id":"key-3","name":"c"}`))
	ts.Delete([]byte("key-3"), seq3) // delete key-3

	seqNum, pks, records, err := ts.SnapshotForWatch()
	if err != nil {
		t.Fatalf("SnapshotForWatch: %v", err)
	}
	if seqNum != 4 { // 3 creates + 1 delete
		t.Fatalf("seqNum: expected 4, got %d", seqNum)
	}
	if len(pks) != 2 { // key-3 was deleted
		t.Fatalf("expected 2 records, got %d", len(pks))
	}
	if string(pks[0]) != "key-1" || string(pks[1]) != "key-2" {
		t.Fatalf("wrong PKs: %v", pks)
	}
	if string(records[0].Data) != `{"id":"key-1","name":"a"}` {
		t.Fatalf("wrong data for key-1: %q", records[0].Data)
	}
}
