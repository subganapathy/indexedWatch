package lsm

import (
	"fmt"
	"os"

	"github.com/subganapathy/indexedwatch/pkg/lsm/sstable"
)

// compactionPick describes which level to compact and the input files.
type compactionPick struct {
	level       int          // source level
	outputLevel int          // target level (level + 1)
	inputs      []*FileMeta  // source level files
	overlaps    []*FileMeta  // target level files that overlap
}

// pickCompaction selects the best compaction to run based on level scores.
// Returns nil if no compaction is needed.
func (db *DB) pickCompaction() *compactionPick {
	v := db.versions.currentVersion()
	defer v.unref()

	// L0 compaction: triggered by file count.
	if len(v.Levels[0]) >= L0CompactionThreshold {
		return db.pickL0Compaction(v)
	}

	// Ln→Ln+1 compaction: triggered by size exceeding target.
	for level := 1; level < NumLevels-1; level++ {
		target := db.targetLevelSize(level)
		if v.levelSize(level) > target {
			return db.pickLevelCompaction(v, level)
		}
	}

	return nil
}

// pickL0Compaction selects all L0 files plus overlapping L1 files.
func (db *DB) pickL0Compaction(v *Version) *compactionPick {
	inputs := make([]*FileMeta, len(v.Levels[0]))
	copy(inputs, v.Levels[0])

	// Find the key range of all L0 files.
	smallest, largest := keyRange(inputs)

	// L0 files may overlap each other, so expand to cover all overlapping L0 files.
	// (In our case we already take all L0 files.)
	overlaps := v.overlapping(1, smallest, largest)

	return &compactionPick{
		level:       0,
		outputLevel: 1,
		inputs:      inputs,
		overlaps:    overlaps,
	}
}

// pickLevelCompaction picks one file from level and its overlapping files in level+1.
func (db *DB) pickLevelCompaction(v *Version, level int) *compactionPick {
	if len(v.Levels[level]) == 0 {
		return nil
	}

	// Pick the file with the largest size.
	var pick *FileMeta
	for _, f := range v.Levels[level] {
		if pick == nil || f.Size > pick.Size {
			pick = f
		}
	}

	inputs := []*FileMeta{pick}
	overlaps := v.overlapping(level+1, pick.SmallestKey, pick.LargestKey)

	return &compactionPick{
		level:       level,
		outputLevel: level + 1,
		inputs:      inputs,
		overlaps:    overlaps,
	}
}

// runCompaction executes a compaction, merging input files and writing new output files.
func (db *DB) runCompaction(pick *compactionPick) error {
	// Collect all input iterators.
	var iters []Iterator
	for _, f := range pick.inputs {
		it, err := db.openTableIterator(f)
		if err != nil {
			return fmt.Errorf("compaction: open input %d: %w", f.FileNum, err)
		}
		iters = append(iters, it)
	}
	for _, f := range pick.overlaps {
		it, err := db.openTableIterator(f)
		if err != nil {
			return fmt.Errorf("compaction: open overlap %d: %w", f.FileNum, err)
		}
		iters = append(iters, it)
	}

	merge := NewMergeIterator(iters)

	// Write output SSTables.
	var outputFiles []FileMeta
	var currentWriter *sstable.Writer
	var currentFile *os.File
	var currentFileNum uint64
	var outputSize uint64

	finishOutput := func() error {
		if currentWriter == nil {
			return nil
		}
		meta, err := currentWriter.Close()
		if err != nil {
			return err
		}
		if err := currentFile.Sync(); err != nil {
			return err
		}
		if err := currentFile.Close(); err != nil {
			return err
		}
		if meta.KeyCount > 0 {
			outputFiles = append(outputFiles, FileMeta{
				FileNum:     currentFileNum,
				Level:       pick.outputLevel,
				Size:        meta.Size,
				SmallestKey: meta.SmallestKey,
				LargestKey:  meta.LargestKey,
			})
		}
		currentWriter = nil
		currentFile = nil
		outputSize = 0
		return nil
	}

	targetSize := db.targetFileSize(pick.outputLevel)

	for merge.First(); merge.Valid(); merge.Next() {
		if currentWriter == nil || outputSize >= targetSize {
			if err := finishOutput(); err != nil {
				return fmt.Errorf("compaction: finish output: %w", err)
			}
			// Start new output file.
			currentFileNum = db.versions.allocFileNum()
			path := db.sstablePath(currentFileNum)
			var err error
			currentFile, err = os.Create(path)
			if err != nil {
				return fmt.Errorf("compaction: create output: %w", err)
			}
			currentWriter = sstable.NewWriter(currentFile, db.opts.TableOptions)
			outputSize = 0
		}

		key := merge.Key()
		val := merge.Value()
		if err := currentWriter.Add(key, val); err != nil {
			return fmt.Errorf("compaction: add: %w", err)
		}
		outputSize += uint64(len(key) + len(val))
	}

	if err := finishOutput(); err != nil {
		return fmt.Errorf("compaction: finish last output: %w", err)
	}

	// Build version edit.
	edit := &VersionEdit{}
	for _, f := range pick.inputs {
		edit.DeletedFiles = append(edit.DeletedFiles, struct {
			Level   int
			FileNum uint64
		}{pick.level, f.FileNum})
	}
	for _, f := range pick.overlaps {
		edit.DeletedFiles = append(edit.DeletedFiles, struct {
			Level   int
			FileNum uint64
		}{pick.outputLevel, f.FileNum})
	}
	edit.NewFiles = outputFiles

	// Apply atomically.
	if err := db.versions.logAndApply(edit); err != nil {
		return fmt.Errorf("compaction: logAndApply: %w", err)
	}

	// Delete obsolete input files.
	for _, f := range pick.inputs {
		os.Remove(db.sstablePath(f.FileNum))
	}
	for _, f := range pick.overlaps {
		os.Remove(db.sstablePath(f.FileNum))
	}

	return nil
}

// targetLevelSize returns the target size for a level.
func (db *DB) targetLevelSize(level int) uint64 {
	size := uint64(BaseLevelTargetSize)
	for i := 1; i < level; i++ {
		size *= LevelMultiplier
	}
	return size
}

// targetFileSize returns the target file size for a level.
func (db *DB) targetFileSize(level int) uint64 {
	size := uint64(TargetFileSize)
	for i := 0; i < level; i++ {
		size *= 2
	}
	return size
}

// keyRange computes the smallest and largest keys across all files.
func keyRange(files []*FileMeta) (smallest, largest []byte) {
	for _, f := range files {
		if smallest == nil || compareKeys(f.SmallestKey, smallest) < 0 {
			smallest = f.SmallestKey
		}
		if largest == nil || compareKeys(f.LargestKey, largest) > 0 {
			largest = f.LargestKey
		}
	}
	return
}
