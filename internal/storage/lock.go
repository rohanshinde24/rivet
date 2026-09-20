//go:build unix

package storage

import (
	"os"
	"path/filepath"
	"syscall"
)

const lockFileName = "LOCK"

// dirLock is exclusive ownership of a data directory for the lifetime of an
// engine (INV-003-10). The lock is advisory, and it is released by the
// kernel if the process dies, which is what makes a crashed engine's
// directory reopenable.
type dirLock struct {
	file *os.File
}

// acquireDirLock takes the exclusive lock without blocking. A second engine
// over the same directory fails here, before it has mutated any file.
func acquireDirLock(dir string) (*dirLock, error) {
	const op = "acquireDirLock"

	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, wrapError(CodeStorage, op, "open lock file", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, wrapError(CodeLocked, op, "data directory is owned by another engine", err)
	}
	return &dirLock{file: f}, nil
}

func (l *dirLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	cerr := l.file.Close()
	l.file = nil
	if err != nil {
		return err
	}
	return cerr
}
