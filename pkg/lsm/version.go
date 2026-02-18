package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	// NumLevels is the number of levels in the LSM tree (L0–L6).
	NumLevels = 7

	// L0CompactionThreshold triggers compaction when L0 has this many files.
	L0CompactionThreshold = 4

	// LevelMultiplier is the growth factor between adjacent level target sizes.
	LevelMultiplier = 10

	// BaseLevelTargetSize is the target size for the base level (L1) in bytes.
	BaseLevelTargetSize = 4 * 1024 * 1024 // 4MB

	// TargetFileSize is the target SSTable file size for L0/L1.
	// Each subsequent level doubles this.
	TargetFileSize = 2 * 1024 * 1024 // 2MB

	// MaxGrandparentOverlap limits overlap with grandparent level during compaction.
	MaxGrandparentOverlap = 10 * TargetFileSize

	manifestName = "MANIFEST"
)

// FileMeta describes a live SSTable file in the LSM tree.
type FileMeta struct {
	FileNum     uint64
	Level       int
	Size        uint64
	SmallestKey []byte
	LargestKey  []byte
}

// VersionEdit describes a delta from one version to the next.
// Adding and deleting files, updating sequence numbers.
type VersionEdit struct {
	NewFiles     []FileMeta
	DeletedFiles []struct {
		Level   int
		FileNum uint64
	}
	LastSeqNum       uint64
	NextFileNum      uint64
	HasLastSeqNum    bool
	HasNextFileNum   bool
}

// encode serializes the VersionEdit to bytes.
func (ve *VersionEdit) encode() []byte {
	var buf []byte
	tmp := make([]byte, binary.MaxVarintLen64)

	if ve.HasLastSeqNum {
		buf = append(buf, 1) // tag: lastSeqNum
		n := binary.PutUvarint(tmp, ve.LastSeqNum)
		buf = append(buf, tmp[:n]...)
	}
	if ve.HasNextFileNum {
		buf = append(buf, 2) // tag: nextFileNum
		n := binary.PutUvarint(tmp, ve.NextFileNum)
		buf = append(buf, tmp[:n]...)
	}
	for _, f := range ve.NewFiles {
		buf = append(buf, 3) // tag: newFile
		n := binary.PutUvarint(tmp, uint64(f.Level))
		buf = append(buf, tmp[:n]...)
		n = binary.PutUvarint(tmp, f.FileNum)
		buf = append(buf, tmp[:n]...)
		n = binary.PutUvarint(tmp, f.Size)
		buf = append(buf, tmp[:n]...)
		n = binary.PutUvarint(tmp, uint64(len(f.SmallestKey)))
		buf = append(buf, tmp[:n]...)
		buf = append(buf, f.SmallestKey...)
		n = binary.PutUvarint(tmp, uint64(len(f.LargestKey)))
		buf = append(buf, tmp[:n]...)
		buf = append(buf, f.LargestKey...)
	}
	for _, d := range ve.DeletedFiles {
		buf = append(buf, 4) // tag: deletedFile
		n := binary.PutUvarint(tmp, uint64(d.Level))
		buf = append(buf, tmp[:n]...)
		n = binary.PutUvarint(tmp, d.FileNum)
		buf = append(buf, tmp[:n]...)
	}

	return buf
}

// decodeVersionEdit deserializes a VersionEdit.
func decodeVersionEdit(data []byte) (*VersionEdit, error) {
	ve := &VersionEdit{}
	i := 0
	for i < len(data) {
		tag := data[i]
		i++
		switch tag {
		case 1: // lastSeqNum
			v, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad lastSeqNum")
			}
			ve.LastSeqNum = v
			ve.HasLastSeqNum = true
			i += n
		case 2: // nextFileNum
			v, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad nextFileNum")
			}
			ve.NextFileNum = v
			ve.HasNextFileNum = true
			i += n
		case 3: // newFile
			var f FileMeta
			v, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad newFile level")
			}
			f.Level = int(v)
			i += n
			f.FileNum, n = binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad newFile fileNum")
			}
			i += n
			f.Size, n = binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad newFile size")
			}
			i += n
			keyLen, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad newFile smallestKey len")
			}
			i += n
			f.SmallestKey = make([]byte, keyLen)
			copy(f.SmallestKey, data[i:i+int(keyLen)])
			i += int(keyLen)
			keyLen, n = binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad newFile largestKey len")
			}
			i += n
			f.LargestKey = make([]byte, keyLen)
			copy(f.LargestKey, data[i:i+int(keyLen)])
			i += int(keyLen)
			ve.NewFiles = append(ve.NewFiles, f)
		case 4: // deletedFile
			v, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad deletedFile level")
			}
			level := int(v)
			i += n
			fileNum, n := binary.Uvarint(data[i:])
			if n <= 0 {
				return nil, fmt.Errorf("manifest: bad deletedFile fileNum")
			}
			i += n
			ve.DeletedFiles = append(ve.DeletedFiles, struct {
				Level   int
				FileNum uint64
			}{level, fileNum})
		default:
			return nil, fmt.Errorf("manifest: unknown tag %d", tag)
		}
	}
	return ve, nil
}

// Version is an immutable snapshot of the LSM tree structure.
// Each level contains a sorted list of live SSTable file metadata.
type Version struct {
	Levels [NumLevels][]*FileMeta
	refs   atomic.Int32
}

func newVersion() *Version {
	v := &Version{}
	v.refs.Store(1)
	return v
}

func (v *Version) ref() { v.refs.Add(1) }

func (v *Version) unref() bool { return v.refs.Add(-1) == 0 }

// clone creates a mutable copy for building the next version.
func (v *Version) clone() *Version {
	nv := &Version{}
	nv.refs.Store(1)
	for l := range NumLevels {
		nv.Levels[l] = make([]*FileMeta, len(v.Levels[l]))
		copy(nv.Levels[l], v.Levels[l])
	}
	return nv
}

// apply applies a VersionEdit, returning a new Version.
func (v *Version) apply(edit *VersionEdit) *Version {
	nv := v.clone()

	// Delete files.
	deleted := make(map[uint64]bool)
	for _, d := range edit.DeletedFiles {
		deleted[d.FileNum] = true
	}
	for l := range NumLevels {
		if len(nv.Levels[l]) == 0 {
			continue
		}
		filtered := nv.Levels[l][:0]
		for _, f := range nv.Levels[l] {
			if !deleted[f.FileNum] {
				filtered = append(filtered, f)
			}
		}
		nv.Levels[l] = filtered
	}

	// Add new files.
	for i := range edit.NewFiles {
		f := &edit.NewFiles[i]
		fm := &FileMeta{
			FileNum:     f.FileNum,
			Level:       f.Level,
			Size:        f.Size,
			SmallestKey: f.SmallestKey,
			LargestKey:  f.LargestKey,
		}
		nv.Levels[f.Level] = append(nv.Levels[f.Level], fm)
	}

	// Sort L1+ by smallest key (L0 stays in insertion order).
	for l := 1; l < NumLevels; l++ {
		sort.Slice(nv.Levels[l], func(i, j int) bool {
			return compareKeys(nv.Levels[l][i].SmallestKey, nv.Levels[l][j].SmallestKey) < 0
		})
	}

	return nv
}

// overlapping returns files in level that overlap [smallest, largest].
func (v *Version) overlapping(level int, smallest, largest []byte) []*FileMeta {
	var result []*FileMeta
	for _, f := range v.Levels[level] {
		if compareKeys(f.SmallestKey, largest) > 0 || compareKeys(f.LargestKey, smallest) < 0 {
			continue
		}
		result = append(result, f)
	}
	return result
}

// levelSize returns the total size of all files in a level.
func (v *Version) levelSize(level int) uint64 {
	var total uint64
	for _, f := range v.Levels[level] {
		total += f.Size
	}
	return total
}

// compareKeys does lexicographic byte comparison.
func compareKeys(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// VersionSet manages the current version and MANIFEST file.
type VersionSet struct {
	mu          sync.Mutex
	current     *Version
	nextFileNum atomic.Uint64
	lastSeqNum  atomic.Uint64

	manifestFile *os.File
	dir          string
}

func newVersionSet(dir string) *VersionSet {
	vs := &VersionSet{
		current: newVersion(),
		dir:     dir,
	}
	vs.nextFileNum.Store(1)
	return vs
}

// currentVersion returns the current version (caller must ref/unref).
func (vs *VersionSet) currentVersion() *Version {
	vs.mu.Lock()
	v := vs.current
	v.ref()
	vs.mu.Unlock()
	return v
}

// allocFileNum allocates a new file number.
func (vs *VersionSet) allocFileNum() uint64 {
	return vs.nextFileNum.Add(1) - 1
}

// logAndApply writes a VersionEdit to the MANIFEST and applies it.
func (vs *VersionSet) logAndApply(edit *VersionEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Update metadata in edit.
	edit.HasNextFileNum = true
	edit.NextFileNum = vs.nextFileNum.Load()
	edit.HasLastSeqNum = true
	edit.LastSeqNum = vs.lastSeqNum.Load()

	// Write to MANIFEST.
	data := edit.encode()
	record := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(record, uint32(len(data)))
	copy(record[4:], data)

	if _, err := vs.manifestFile.Write(record); err != nil {
		return fmt.Errorf("manifest: write failed: %w", err)
	}
	if err := vs.manifestFile.Sync(); err != nil {
		return fmt.Errorf("manifest: sync failed: %w", err)
	}

	// Apply to current version.
	newVersion := vs.current.apply(edit)
	vs.current.unref()
	vs.current = newVersion

	return nil
}

// recover reads the MANIFEST and reconstructs the version set.
func (vs *VersionSet) recover() error {
	path := filepath.Join(vs.dir, manifestName)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// No manifest — fresh start.
		return vs.createManifest()
	}
	if err != nil {
		return fmt.Errorf("manifest: open failed: %w", err)
	}
	defer f.Close()

	v := newVersion()
	for {
		var lenBuf [4]byte
		_, err := io.ReadFull(f, lenBuf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("manifest: read length failed: %w", err)
		}

		recordLen := binary.LittleEndian.Uint32(lenBuf[:])
		data := make([]byte, recordLen)
		if _, err := io.ReadFull(f, data); err != nil {
			return fmt.Errorf("manifest: read record failed: %w", err)
		}

		edit, err := decodeVersionEdit(data)
		if err != nil {
			return err
		}

		v = v.apply(edit)
		if edit.HasNextFileNum && edit.NextFileNum > vs.nextFileNum.Load() {
			vs.nextFileNum.Store(edit.NextFileNum)
		}
		if edit.HasLastSeqNum {
			vs.lastSeqNum.Store(edit.LastSeqNum)
		}
	}

	vs.current.unref()
	vs.current = v

	// Reopen manifest for appending.
	return vs.createManifest()
}

func (vs *VersionSet) createManifest() error {
	path := filepath.Join(vs.dir, manifestName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("manifest: create failed: %w", err)
	}
	vs.manifestFile = f
	return nil
}

func (vs *VersionSet) close() error {
	if vs.manifestFile != nil {
		return vs.manifestFile.Close()
	}
	return nil
}
