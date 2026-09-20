package storage

import (
	"io"
	"os"
)

// ioHooks indirects every storage operation whose failure SPEC-003 requires
// tests to exercise: short writes, disk-full, sync, rename, truncate, and
// directory-sync errors.
//
// The type and its field are unexported and are only ever replaced from tests
// in this package. No exported API, configuration key, or network surface can
// reach it, which is the constraint SPEC-003 open question 5 asks for.
type ioHooks struct {
	Write    func(f *os.File, b []byte) (int, error)
	Sync     func(f *os.File) error
	Truncate func(f *os.File, size int64) error
	Rename   func(oldPath, newPath string) error
	Remove   func(name string) error
	SyncDir  func(dir string) error
}

func defaultHooks() *ioHooks {
	return &ioHooks{
		Write:    func(f *os.File, b []byte) (int, error) { return f.Write(b) },
		Sync:     func(f *os.File) error { return f.Sync() },
		Truncate: func(f *os.File, size int64) error { return f.Truncate(size) },
		Rename:   os.Rename,
		Remove:   os.Remove,
		SyncDir:  syncDir,
	}
}

// syncDir synchronizes a directory so that a create, rename, or unlink of a
// directory entry is durable. Skipping this is the classic way to lose a file
// that fsync already reported as safe.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// writeFull writes every byte of b, looping over partial writes. It never
// assumes one Write call is complete (SPEC-003 "WAL record frame version 1").
func (h *ioHooks) writeFull(f *os.File, b []byte) (int, error) {
	written := 0
	for len(b) > 0 {
		n, err := h.Write(f, b)
		if n < 0 || n > len(b) {
			return written, io.ErrShortWrite
		}
		written += n
		b = b[n:]
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
