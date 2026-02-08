package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/subganapathy/indexedwatch/pkg/lsm"
)

const (
	snapshotMagic   = "SNAP"
	snapshotVersion = 1
	snapshotDir     = "snapshots"
	snapshotMeta    = "snapshot.meta"
)

// SnapshotMeta holds metadata about a snapshot file.
type SnapshotMeta struct {
	// SeqNum is the storage seqNum at the time the snapshot was taken.
	// All records in the snapshot have UpdateSeqNum <= SeqNum.
	SeqNum uint64 `json:"seq_num"`

	// WALSegmentSeq is the WAL segment that was active at snapshot time.
	// Segments before this can be safely pruned after the snapshot is complete.
	WALSegmentSeq uint64 `json:"wal_segment_seq"`

	// Path is the filesystem path to the snapshot data file.
	Path string `json:"path"`
}

// Snapshot file format:
//
//	[4 bytes: magic "SNAP"]
//	[4 bytes: version (1)]
//	[8 bytes: seqNum]
//	For each record:
//	  [4 bytes: PK length]
//	  [PK bytes]
//	  [4 bytes: value length]
//	  [value bytes (StoredRecord marshaled)]
//	Terminator:
//	  [4 bytes: 0 (zero PK length signals end)]
//	  [4 bytes: CRC-32C of all preceding bytes]

// takeSnapshot iterates the Primary LSM and writes all non-deleted records
// to a snapshot file. Returns the path to the snapshot file.
//
// The caller should hold the TypeStore write lock briefly to capture the seqNum,
// then call this with the seqNum and an iterator from that point-in-time.
func takeSnapshot(it *lsm.DBIterator, dir string, seqNum uint64) (string, error) {
	snapDir := filepath.Join(dir, snapshotDir)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return "", fmt.Errorf("snapshot: mkdir: %w", err)
	}

	filename := fmt.Sprintf("snapshot-%016d.dat", seqNum)
	path := filepath.Join(snapDir, filename)

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("snapshot: create: %w", err)
	}
	defer f.Close()

	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	// Write header.
	header := make([]byte, 16)
	copy(header[0:4], snapshotMagic)
	binary.BigEndian.PutUint32(header[4:8], snapshotVersion)
	binary.BigEndian.PutUint64(header[8:16], seqNum)
	if _, err := f.Write(header); err != nil {
		return "", fmt.Errorf("snapshot: write header: %w", err)
	}
	crc.Write(header)

	// Write records.
	buf := make([]byte, 4) // reusable length buffer

	if it.First() {
		for it.Valid() {
			pk := it.Key()
			val := it.Value()

			// Skip deleted records.
			record, err := UnmarshalStoredRecord(val)
			if err != nil {
				return "", fmt.Errorf("snapshot: unmarshal: %w", err)
			}
			if record.IsDeleted {
				if !it.Next() {
					break
				}
				continue
			}

			// Write PK length + PK.
			binary.BigEndian.PutUint32(buf, uint32(len(pk)))
			f.Write(buf)
			crc.Write(buf)
			f.Write(pk)
			crc.Write(pk)

			// Write value length + value.
			binary.BigEndian.PutUint32(buf, uint32(len(val)))
			f.Write(buf)
			crc.Write(buf)
			f.Write(val)
			crc.Write(val)

			if !it.Next() {
				break
			}
		}
	}

	// Write terminator (zero PK length).
	binary.BigEndian.PutUint32(buf, 0)
	f.Write(buf)
	crc.Write(buf)

	// Write CRC.
	binary.BigEndian.PutUint32(buf, crc.Sum32())
	f.Write(buf)

	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("snapshot: sync: %w", err)
	}

	return path, nil
}

// loadSnapshot reads a snapshot file and replays all records into the LSM.
// Returns the snapshot's seqNum.
func loadSnapshot(path string, db *lsm.DB) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("snapshot: open: %w", err)
	}
	defer f.Close()

	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	// Read header.
	header := make([]byte, 16)
	if _, err := readFull(f, header); err != nil {
		return 0, fmt.Errorf("snapshot: read header: %w", err)
	}
	crc.Write(header)

	if string(header[0:4]) != snapshotMagic {
		return 0, fmt.Errorf("snapshot: invalid magic: %q", header[0:4])
	}
	version := binary.BigEndian.Uint32(header[4:8])
	if version != snapshotVersion {
		return 0, fmt.Errorf("snapshot: unknown version: %d", version)
	}
	seqNum := binary.BigEndian.Uint64(header[8:16])

	// Read records.
	buf := make([]byte, 4)
	for {
		// Read PK length.
		if _, err := readFull(f, buf); err != nil {
			return 0, fmt.Errorf("snapshot: read pk len: %w", err)
		}
		crc.Write(buf)
		pkLen := binary.BigEndian.Uint32(buf)

		if pkLen == 0 {
			break // terminator
		}

		// Read PK.
		pk := make([]byte, pkLen)
		if _, err := readFull(f, pk); err != nil {
			return 0, fmt.Errorf("snapshot: read pk: %w", err)
		}
		crc.Write(pk)

		// Read value length.
		if _, err := readFull(f, buf); err != nil {
			return 0, fmt.Errorf("snapshot: read val len: %w", err)
		}
		crc.Write(buf)
		valLen := binary.BigEndian.Uint32(buf)

		// Read value.
		val := make([]byte, valLen)
		if _, err := readFull(f, val); err != nil {
			return 0, fmt.Errorf("snapshot: read val: %w", err)
		}
		crc.Write(val)

		// Set in LSM.
		if err := db.Set(pk, val); err != nil {
			return 0, fmt.Errorf("snapshot: lsm set: %w", err)
		}
	}

	// Verify CRC.
	if _, err := readFull(f, buf); err != nil {
		return 0, fmt.Errorf("snapshot: read crc: %w", err)
	}
	expectedCRC := binary.BigEndian.Uint32(buf)
	actualCRC := crc.Sum32()
	if expectedCRC != actualCRC {
		return 0, fmt.Errorf("snapshot: CRC mismatch: expected %d, got %d", expectedCRC, actualCRC)
	}

	return seqNum, nil
}

// writeSnapshotMeta writes snapshot metadata to a JSON file.
func writeSnapshotMeta(dir string, meta *SnapshotMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("snapshot: marshal meta: %w", err)
	}

	path := filepath.Join(dir, snapshotMeta)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("snapshot: write meta: %w", err)
	}
	return nil
}

// readSnapshotMeta reads snapshot metadata, returning nil if no snapshot exists.
func readSnapshotMeta(dir string) *SnapshotMeta {
	path := filepath.Join(dir, snapshotMeta)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	var meta SnapshotMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil
	}
	return &meta
}

// removeOldSnapshots deletes snapshot files that are older than the current one.
func removeOldSnapshots(dir string, currentPath string) {
	snapDir := filepath.Join(dir, snapshotDir)
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		path := filepath.Join(snapDir, entry.Name())
		if path != currentPath {
			os.Remove(path)
		}
	}
}

// readFull reads exactly len(buf) bytes from f.
func readFull(f *os.File, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		nn, err := f.Read(buf[n:])
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
