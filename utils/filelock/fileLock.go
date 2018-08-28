package filelock

import (
	"fmt"
	"os"
	"syscall"
)

type FileLock struct {
	path string
	f   *os.File
}

func NewFileLock(path string) *FileLock {
	return &FileLock{
		path: path,
	}
}

func (l *FileLock) TryLock() error {
	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s", l.path, err)
	}
	return nil
}

func (l *FileLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}