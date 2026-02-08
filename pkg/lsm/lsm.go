package lsm

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/subganapathy/indexedwatch/pkg/lsm/memtable"
	"github.com/subganapathy/indexedwatch/pkg/lsm/sstable"
)

var (
	// ErrClosed is returned when operating on a closed database.
	ErrClosed = errors.New("lsm: database closed")
	// ErrNotFound is returned when a key is not found.
	ErrNotFound = errors.New("lsm: key not found")
)

const (
	// DefaultMemtableSize is the default arena size for memtables (64 MB).
	DefaultMemtableSize = 64 * 1024 * 1024
	// DefaultBlockCacheSize is the default shared block cache size (128 MB).
	DefaultBlockCacheSize = 128 * 1024 * 1024
	// maxSeqNum is the maximum sequence number (56 bits).
	maxSeqNum = (1 << 56) - 1
)

// Options configures the LSM database.
type Options struct {
	// MemtableSize is the arena size for each memtable in bytes.
	MemtableSize int
	// TableOptions configures SSTable writer behavior.
	TableOptions sstable.WriterOptions
	// BlockCacheSize is the shared block cache capacity in bytes.
	BlockCacheSize int
}

func (o *Options) ensureDefaults() {
	if o.MemtableSize <= 0 {
		o.MemtableSize = DefaultMemtableSize
	}
	if o.BlockCacheSize <= 0 {
		o.BlockCacheSize = DefaultBlockCacheSize
	}
	if o.TableOptions.BlockSize <= 0 {
		o.TableOptions = sstable.DefaultWriterOptions()
	}
}

// tableEntry is a cached open SSTable reader + file handle.
type tableEntry struct {
	reader *sstable.Reader
	file   *os.File
}

// DB is an LSM-tree key-value store. It provides concurrent reads and writes,
// with background flush and multi-level compaction (L0 through L6).
//
// Write path: active memtable → seal when full → background flush to L0 SSTable.
// Read path: active memtable → sealed memtables → SSTables by level.
//
// Keys stored internally include an 8-byte trailer encoding sequence number
// and operation kind (set/delete) for MVCC.
type DB struct {
	dir      string
	opts     Options
	versions *VersionSet
	cache    *sstable.BlockCache

	// mu protects mem and imm.
	mu  sync.Mutex
	mem *memtable.Memtable   // active memtable
	imm []*memtable.Memtable // sealed memtables awaiting flush (oldest first)

	seqNum atomic.Uint64 // last allocated sequence number

	// tableMu protects tableCache.
	tableMu    sync.RWMutex
	tableCache map[uint64]*tableEntry

	flushCh   chan struct{}
	compactCh chan struct{}
	closeCh   chan struct{}
	closeWG   sync.WaitGroup
	closed    atomic.Bool
}

// Open opens or creates an LSM database at the given directory.
func Open(dir string, opts Options) (*DB, error) {
	opts.ensureDefaults()

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir: %w", err)
	}

	db := &DB{
		dir:        dir,
		opts:       opts,
		versions:   newVersionSet(dir),
		cache:      sstable.NewBlockCache(opts.BlockCacheSize),
		tableCache: make(map[uint64]*tableEntry),
		flushCh:    make(chan struct{}, 1),
		compactCh:  make(chan struct{}, 1),
		closeCh:    make(chan struct{}),
	}

	// Recover version set from MANIFEST.
	if err := db.versions.recover(); err != nil {
		return nil, fmt.Errorf("lsm: recover: %w", err)
	}

	// Restore seqNum from version set.
	db.seqNum.Store(db.versions.lastSeqNum.Load())

	// Create initial memtable.
	db.mem = memtable.New(opts.MemtableSize)

	// Start background goroutines.
	db.closeWG.Add(2)
	go db.flushLoop()
	go db.compactionLoop()

	return db, nil
}

// Set inserts or updates a key-value pair.
func (db *DB) Set(key, value []byte) error {
	if db.closed.Load() {
		return ErrClosed
	}

	seq := db.seqNum.Add(1)
	db.versions.lastSeqNum.Store(seq)

	db.mu.Lock()
	err := db.mem.Set(key, value, seq)
	if err != nil {
		// Arena full — rotate memtable.
		db.rotateMemtable()
		err = db.mem.Set(key, value, seq)
	}
	shouldFlush := len(db.imm) > 0
	db.mu.Unlock()

	if shouldFlush {
		db.signalFlush()
	}
	return err
}

// Delete inserts a tombstone for the key.
func (db *DB) Delete(key []byte) error {
	if db.closed.Load() {
		return ErrClosed
	}

	seq := db.seqNum.Add(1)
	db.versions.lastSeqNum.Store(seq)

	db.mu.Lock()
	err := db.mem.Delete(key, seq)
	if err != nil {
		db.rotateMemtable()
		err = db.mem.Delete(key, seq)
	}
	shouldFlush := len(db.imm) > 0
	db.mu.Unlock()

	if shouldFlush {
		db.signalFlush()
	}
	return err
}

// Get looks up a key. Returns the value or ErrNotFound.
// Reads are served at the latest sequence number.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	seq := db.seqNum.Load()

	// Snapshot the memtable references.
	db.mu.Lock()
	mem := db.mem
	mem.Ref()
	imms := make([]*memtable.Memtable, len(db.imm))
	copy(imms, db.imm)
	for _, m := range imms {
		m.Ref()
	}
	db.mu.Unlock()

	defer func() {
		mem.Unref()
		for _, m := range imms {
			m.Unref()
		}
	}()

	// Check active memtable.
	val, kind, ok := getFromMemtable(mem, key, seq)
	if ok {
		if kind == memtable.KindDelete {
			return nil, ErrNotFound
		}
		return val, nil
	}

	// Check sealed memtables (newest first).
	for i := len(imms) - 1; i >= 0; i-- {
		val, kind, ok = getFromMemtable(imms[i], key, seq)
		if ok {
			if kind == memtable.KindDelete {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	// Check SSTables.
	v := db.versions.currentVersion()
	defer v.unref()

	return db.getFromVersion(v, key, seq)
}

// getFromMemtable searches a memtable for a key at the given sequence number.
// Returns (value, kind, found). Unlike memtable.Get, this distinguishes
// "not found" from "found as tombstone".
func getFromMemtable(m *memtable.Memtable, key []byte, seq uint64) ([]byte, memtable.Kind, bool) {
	seekKey := memtable.MakeInternalKey(key, seq, memtable.KindSet)
	it := m.NewIterator()
	if !it.SeekGE(seekKey) {
		return nil, 0, false
	}
	if compareKeys(it.UserKey(), key) != 0 {
		return nil, 0, false
	}
	return it.Value(), it.Kind(), true
}

// getFromVersion searches SSTables in the given version for a key.
func (db *DB) getFromVersion(v *Version, key []byte, seq uint64) ([]byte, error) {
	seekKey := memtable.MakeInternalKey(key, seq, memtable.KindSet)

	// L0: check all files (they may overlap). Newest files are at the end.
	for i := len(v.Levels[0]) - 1; i >= 0; i-- {
		f := v.Levels[0][i]
		if compareKeys(key, memtable.UserKey(f.SmallestKey)) < 0 ||
			compareKeys(key, memtable.UserKey(f.LargestKey)) > 0 {
			continue
		}
		val, kind, err := db.getFromTable(f, seekKey, key)
		if err != nil {
			return nil, err
		}
		if kind == memtable.KindDelete {
			return nil, ErrNotFound
		}
		if val != nil {
			return val, nil
		}
	}

	// L1+: files are sorted by key range and non-overlapping.
	for level := 1; level < NumLevels; level++ {
		files := v.Levels[level]
		if len(files) == 0 {
			continue
		}

		// Binary search for the file whose largest key >= our key.
		idx := sort.Search(len(files), func(i int) bool {
			return compareKeys(memtable.UserKey(files[i].LargestKey), key) >= 0
		})
		if idx >= len(files) {
			continue
		}
		f := files[idx]
		if compareKeys(key, memtable.UserKey(f.SmallestKey)) < 0 {
			continue
		}

		val, kind, err := db.getFromTable(f, seekKey, key)
		if err != nil {
			return nil, err
		}
		if kind == memtable.KindDelete {
			return nil, ErrNotFound
		}
		if val != nil {
			return val, nil
		}
	}

	return nil, ErrNotFound
}

// getFromTable seeks in a single SSTable for the given key.
// Returns (value, kind, error). value is nil if the key is not in this table.
func (db *DB) getFromTable(f *FileMeta, seekKey, userKey []byte) ([]byte, memtable.Kind, error) {
	reader, err := db.getTableReader(f)
	if err != nil {
		return nil, 0, err
	}

	it := reader.NewIterator()
	if !it.SeekGE(seekKey) {
		return nil, 0, nil // not found in this table
	}

	foundUserKey := memtable.UserKey(it.Key())
	if compareKeys(foundUserKey, userKey) != 0 {
		return nil, 0, nil // not found in this table
	}

	_, _, kind := memtable.ParseInternalKey(it.Key())
	if kind == memtable.KindDelete {
		return nil, memtable.KindDelete, nil
	}

	val := make([]byte, len(it.Value()))
	copy(val, it.Value())
	return val, memtable.KindSet, nil
}

// getTableReader returns a cached SSTable reader, opening the file if needed.
func (db *DB) getTableReader(f *FileMeta) (*sstable.Reader, error) {
	db.tableMu.RLock()
	tr, ok := db.tableCache[f.FileNum]
	db.tableMu.RUnlock()
	if ok {
		return tr.reader, nil
	}

	db.tableMu.Lock()
	defer db.tableMu.Unlock()

	// Double-check after acquiring write lock.
	if tr, ok := db.tableCache[f.FileNum]; ok {
		return tr.reader, nil
	}

	path := db.sstablePath(f.FileNum)
	reader, file, err := sstable.OpenReader(path, sstable.ReaderOptions{
		Cache:  db.cache,
		FileID: f.FileNum,
	})
	if err != nil {
		return nil, fmt.Errorf("lsm: open table %d: %w", f.FileNum, err)
	}

	db.tableCache[f.FileNum] = &tableEntry{reader: reader, file: file}
	return reader, nil
}

// openTableIterator opens an SSTable iterator. Used by compaction.
func (db *DB) openTableIterator(f *FileMeta) (Iterator, error) {
	reader, err := db.getTableReader(f)
	if err != nil {
		return nil, err
	}
	return reader.NewIterator(), nil
}

// NewIterator returns a forward iterator over all live key-value pairs.
// The iterator merges data from memtables and SSTables, resolving MVCC
// (only the newest version of each key is visible) and filtering tombstones.
// The caller must call Close() when done.
func (db *DB) NewIterator() *DBIterator {
	// Snapshot memtable references.
	db.mu.Lock()
	mem := db.mem
	mem.Ref()
	imms := make([]*memtable.Memtable, len(db.imm))
	copy(imms, db.imm)
	for _, m := range imms {
		m.Ref()
	}
	db.mu.Unlock()

	v := db.versions.currentVersion()

	// Build iterator list: newest source first (index 0 = newest).
	var iters []Iterator

	// Active memtable (newest).
	iters = append(iters, &memIterAdapter{it: mem.NewIterator()})

	// Sealed memtables (newest to oldest).
	for i := len(imms) - 1; i >= 0; i-- {
		iters = append(iters, &memIterAdapter{it: imms[i].NewIterator()})
	}

	// L0 SSTables (newest to oldest — files at end are newest).
	for i := len(v.Levels[0]) - 1; i >= 0; i-- {
		f := v.Levels[0][i]
		reader, err := db.getTableReader(f)
		if err != nil {
			continue
		}
		iters = append(iters, reader.NewIterator())
	}

	// L1+ SSTables (one iterator per file, level by level).
	for level := 1; level < NumLevels; level++ {
		for _, f := range v.Levels[level] {
			reader, err := db.getTableReader(f)
			if err != nil {
				continue
			}
			iters = append(iters, reader.NewIterator())
		}
	}

	return &DBIterator{
		merge: NewMergeIterator(iters),
		mem:   mem,
		imm:   imms,
		v:     v,
	}
}

// NewSnapshot returns a point-in-time snapshot of the database.
// Reads through the snapshot see data as of the snapshot's sequence number.
// The caller must call Close() when done.
func (db *DB) NewSnapshot() *Snapshot {
	seq := db.seqNum.Load()
	v := db.versions.currentVersion()

	db.mu.Lock()
	mem := db.mem
	mem.Ref()
	imms := make([]*memtable.Memtable, len(db.imm))
	copy(imms, db.imm)
	for _, m := range imms {
		m.Ref()
	}
	db.mu.Unlock()

	return &Snapshot{
		db:     db,
		seqNum: seq,
		v:      v,
		mem:    mem,
		imm:    imms,
	}
}

// LastAppliedSeqNum returns the highest sequence number written to the database.
func (db *DB) LastAppliedSeqNum() uint64 {
	return db.seqNum.Load()
}

// Close shuts down the database, flushing pending memtables and stopping
// background goroutines.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}

	// Signal background goroutines to stop and wait.
	close(db.closeCh)
	db.closeWG.Wait()

	// Close all cached table readers.
	db.tableMu.Lock()
	for _, tr := range db.tableCache {
		tr.file.Close()
	}
	db.tableCache = nil
	db.tableMu.Unlock()

	return db.versions.close()
}

// rotateMemtable seals the active memtable and creates a new one.
// Must be called with db.mu held.
func (db *DB) rotateMemtable() {
	db.imm = append(db.imm, db.mem)
	db.mem = memtable.New(db.opts.MemtableSize)
}

func (db *DB) signalFlush() {
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}

func (db *DB) signalCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

// flushLoop runs in the background, flushing sealed memtables to L0 SSTables.
func (db *DB) flushLoop() {
	defer db.closeWG.Done()
	for {
		select {
		case <-db.flushCh:
			db.doFlush()
		case <-db.closeCh:
			db.doFlush() // flush remaining before exit
			return
		}
	}
}

func (db *DB) doFlush() {
	for {
		db.mu.Lock()
		if len(db.imm) == 0 {
			db.mu.Unlock()
			return
		}
		mem := db.imm[0]
		db.mu.Unlock()

		fm, err := db.flush(mem)
		if err != nil {
			return
		}

		edit := &VersionEdit{
			NewFiles: []FileMeta{*fm},
		}
		if err := db.versions.logAndApply(edit); err != nil {
			return
		}

		// Remove from immutable list.
		db.mu.Lock()
		db.imm = db.imm[1:]
		db.mu.Unlock()

		mem.Unref()

		// Maybe trigger compaction after adding an L0 file.
		db.signalCompaction()
	}
}

// compactionLoop runs in the background, performing multi-level compaction.
func (db *DB) compactionLoop() {
	defer db.closeWG.Done()
	for {
		select {
		case <-db.compactCh:
			db.doCompaction()
		case <-db.closeCh:
			return
		}
	}
}

func (db *DB) doCompaction() {
	for {
		pick := db.pickCompaction()
		if pick == nil {
			return
		}
		if err := db.runCompaction(pick); err != nil {
			return
		}
		// Clean up block cache entries for deleted files.
		for _, f := range pick.inputs {
			db.cache.Evict(f.FileNum)
		}
		for _, f := range pick.overlaps {
			db.cache.Evict(f.FileNum)
		}
	}
}

// memIterAdapter adapts memtable.Iterator to the lsm.Iterator interface.
// The memtable iterator uses InternalKey() while lsm.Iterator uses Key().
type memIterAdapter struct {
	it *memtable.Iterator
}

func (a *memIterAdapter) SeekGE(key []byte) bool { return a.it.SeekGE(key) }
func (a *memIterAdapter) First() bool            { return a.it.First() }
func (a *memIterAdapter) Next() bool             { return a.it.Next() }
func (a *memIterAdapter) Valid() bool             { return a.it.Valid() }
func (a *memIterAdapter) Key() []byte             { return a.it.InternalKey() }
func (a *memIterAdapter) Value() []byte           { return a.it.Value() }

// DBIterator iterates over user-visible key-value pairs, resolving MVCC
// (only the newest version of each user key) and filtering tombstones.
type DBIterator struct {
	merge *MergeIterator
	key   []byte // current user key
	val   []byte // current value
	valid bool

	// References held by this iterator — released on Close().
	mem *memtable.Memtable
	imm []*memtable.Memtable
	v   *Version
}

// SeekGE positions the iterator at the first user key >= target.
func (it *DBIterator) SeekGE(target []byte) bool {
	// Construct an internal key that sorts before all versions of target.
	seekKey := memtable.MakeInternalKey(target, maxSeqNum, memtable.KindSet)
	if !it.merge.SeekGE(seekKey) {
		it.valid = false
		return false
	}
	// Due to how internal key trailers work, the merge iterator may position
	// at an entry whose user key is < target (a shorter key's trailer bytes
	// can sort after the target's continuation bytes). Skip past those.
	for it.merge.Valid() {
		userKey := memtable.UserKey(it.merge.Key())
		if compareKeys(userKey, target) >= 0 {
			break
		}
		it.skipUserKey(userKey)
	}
	if !it.merge.Valid() {
		it.valid = false
		return false
	}
	return it.findNextVisible()
}

// First positions at the first user key.
func (it *DBIterator) First() bool {
	if !it.merge.First() {
		it.valid = false
		return false
	}
	return it.findNextVisible()
}

// Next advances to the next distinct user key.
func (it *DBIterator) Next() bool {
	if !it.valid {
		return false
	}
	// After findNextVisible, the merge iterator is already positioned
	// at the next different user key (or exhausted).
	if !it.merge.Valid() {
		it.valid = false
		return false
	}
	return it.findNextVisible()
}

// Valid reports whether the iterator is positioned at a valid entry.
func (it *DBIterator) Valid() bool { return it.valid }

// Key returns the current user key.
func (it *DBIterator) Key() []byte { return it.key }

// Value returns the current value.
func (it *DBIterator) Value() []byte { return it.val }

// Close releases all resources held by the iterator.
func (it *DBIterator) Close() {
	if it.mem != nil {
		it.mem.Unref()
		it.mem = nil
	}
	for _, m := range it.imm {
		m.Unref()
	}
	it.imm = nil
	if it.v != nil {
		it.v.unref()
		it.v = nil
	}
	it.valid = false
}

// findNextVisible scans forward to find the next user-visible entry.
// After returning true, the merge iterator is positioned at the first entry
// of the next distinct user key (or exhausted).
func (it *DBIterator) findNextVisible() bool {
	for it.merge.Valid() {
		ikey := it.merge.Key()
		userKey, _, kind := memtable.ParseInternalKey(ikey)

		if kind == memtable.KindSet {
			it.key = append(it.key[:0], userKey...)
			it.val = append(it.val[:0], it.merge.Value()...)
			it.valid = true
			it.skipUserKey(userKey)
			return true
		}

		// Tombstone — skip all remaining entries for this user key.
		it.skipUserKey(userKey)
	}
	it.valid = false
	return false
}

// skipUserKey advances the merge iterator past all entries with the given user key.
func (it *DBIterator) skipUserKey(userKey []byte) {
	for it.merge.Next() {
		nextUserKey := memtable.UserKey(it.merge.Key())
		if compareKeys(nextUserKey, userKey) != 0 {
			return
		}
	}
}

// Snapshot is a point-in-time view of the database at a specific sequence number.
type Snapshot struct {
	db     *DB
	seqNum uint64
	v      *Version
	mem    *memtable.Memtable
	imm    []*memtable.Memtable
	closed bool
}

// Get looks up a key at the snapshot's sequence number.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}

	// Check active memtable.
	val, kind, ok := getFromMemtable(s.mem, key, s.seqNum)
	if ok {
		if kind == memtable.KindDelete {
			return nil, ErrNotFound
		}
		return val, nil
	}

	// Check sealed memtables (newest first).
	for i := len(s.imm) - 1; i >= 0; i-- {
		val, kind, ok = getFromMemtable(s.imm[i], key, s.seqNum)
		if ok {
			if kind == memtable.KindDelete {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	return s.db.getFromVersion(s.v, key, s.seqNum)
}

// SeqNum returns the sequence number at which this snapshot was taken.
func (s *Snapshot) SeqNum() uint64 {
	return s.seqNum
}

// Close releases the snapshot's resources.
func (s *Snapshot) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.v.unref()
	s.mem.Unref()
	for _, m := range s.imm {
		m.Unref()
	}
}
