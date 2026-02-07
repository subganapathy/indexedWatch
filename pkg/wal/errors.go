package wal

import "errors"

var (
	// ErrClosed is returned when operations are attempted on a closed WAL.
	ErrClosed = errors.New("wal: closed")

	// ErrCRCMismatch is returned when a record's CRC doesn't match the expected CRC.
	ErrCRCMismatch = errors.New("wal: crc mismatch")

	// ErrCorrupted is returned when WAL data corruption is detected.
	ErrCorrupted = errors.New("wal: data corrupted")
)
