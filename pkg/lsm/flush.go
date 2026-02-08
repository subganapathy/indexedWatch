package lsm

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/subganapathy/indexedwatch/pkg/lsm/memtable"
	"github.com/subganapathy/indexedwatch/pkg/lsm/sstable"
)

// flush writes a sealed memtable to a new L0 SSTable file.
// Returns the resulting FileMeta or error.
func (db *DB) flush(mem *memtable.Memtable) (*FileMeta, error) {
	fileNum := db.versions.allocFileNum()
	path := db.sstablePath(fileNum)

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("flush: create file: %w", err)
	}

	w := sstable.NewWriter(f, db.opts.TableOptions)
	it := mem.NewIterator()

	for it.First(); it.Valid(); it.Next() {
		if err := w.Add(it.InternalKey(), it.Value()); err != nil {
			f.Close()
			os.Remove(path)
			return nil, fmt.Errorf("flush: add key: %w", err)
		}
	}

	meta, err := w.Close()
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("flush: close writer: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("flush: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("flush: close file: %w", err)
	}

	return &FileMeta{
		FileNum:     fileNum,
		Level:       0,
		Size:        meta.Size,
		SmallestKey: meta.SmallestKey,
		LargestKey:  meta.LargestKey,
	}, nil
}

// sstablePath returns the file path for an SSTable with the given number.
func (db *DB) sstablePath(fileNum uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", fileNum))
}
