package wal

import "io"

const (
	// defaultBufferBytes is the default size for the write buffer.
	defaultBufferBytes = 128 * 1024 // 128KB (same as etcd)
	// defaultPageBytes is the default page size for alignment.
	defaultPageBytes = 4096 // 4KB
)

// PageWriter implements buffered writes with page alignment.
// It buffers writes and flushes on page boundaries to avoid
// fsync per write while maintaining alignment for torn write detection.
type PageWriter struct {
	w io.Writer
	// pageOffset tracks the page offset of the base of the buffer
	pageOffset int
	// pageBytes is the number of bytes per page
	pageBytes int
	// bufferedBytes counts the number of bytes pending for write in the buffer
	bufferedBytes int
	// buf holds the write buffer
	buf []byte
	// bufWatermarkBytes is the number of bytes the buffer can hold before it needs
	// to be flushed. It is less than len(buf) so there is space for slack writes
	// to bring the writer to page alignment.
	bufWatermarkBytes int
}

// NewPageWriter creates a new PageWriter.
// pageBytes is the number of bytes per page (for alignment).
// pageOffset is the starting offset in the underlying writer.
// bufferSize is the buffer size (0 means use default 128KB).
func NewPageWriter(w io.Writer, pageBytes, pageOffset, bufferSize int) *PageWriter {
	if pageBytes <= 0 {
		pageBytes = defaultPageBytes
	}
	if bufferSize <= 0 {
		bufferSize = defaultBufferBytes
	}
	return &PageWriter{
		w:                 w,
		pageOffset:        pageOffset,
		pageBytes:         pageBytes,
		buf:               make([]byte, bufferSize+pageBytes),
		bufWatermarkBytes: bufferSize,
	}
}

// Write writes data to the buffer, flushing to the underlying writer when needed.
func (pw *PageWriter) Write(p []byte) (n int, err error) {
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		// no overflow - just buffer
		copy(pw.buf[pw.bufferedBytes:], p)
		pw.bufferedBytes += len(p)
		return len(p), nil
	}

	// Complete the slack page in the buffer if unaligned
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	if slack != pw.pageBytes {
		partial := slack > len(p)
		if partial {
			// not enough data to complete the slack page
			slack = len(p)
		}
		// special case: writing to slack page in buffer
		copy(pw.buf[pw.bufferedBytes:], p[:slack])
		pw.bufferedBytes += slack
		n = slack
		p = p[slack:]
		if partial {
			// avoid forcing an unaligned flush
			return n, nil
		}
	}

	// Buffer contents are now page-aligned; flush
	if err = pw.Flush(); err != nil {
		return n, err
	}

	// Directly write all complete pages without copying
	if len(p) > pw.pageBytes {
		pages := len(p) / pw.pageBytes
		c, werr := pw.w.Write(p[:pages*pw.pageBytes])
		n += c
		if werr != nil {
			return n, werr
		}
		p = p[pages*pw.pageBytes:]
	}

	// Write remaining tail directly to buffer.
	// After direct page writes, len(p) < pageBytes, and our buffer
	// has capacity for bufferSize+pageBytes, so this always fits.
	// We don't recurse here to avoid infinite recursion when
	// bufferSize < pageBytes.
	if len(p) > 0 {
		copy(pw.buf[pw.bufferedBytes:], p)
		pw.bufferedBytes += len(p)
		n += len(p)
	}
	return n, nil
}

// Flush flushes buffered data to the underlying writer.
func (pw *PageWriter) Flush() error {
	if pw.bufferedBytes == 0 {
		return nil
	}
	_, err := pw.w.Write(pw.buf[:pw.bufferedBytes])
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes
	pw.bufferedBytes = 0
	return err
}

// Buffered returns the number of bytes currently buffered.
func (pw *PageWriter) Buffered() int {
	return pw.bufferedBytes
}
