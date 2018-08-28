package filelock

import (
	"testing"
	"fmt"
	"time"
	"os"
)

func tryLockFunc() {
	fileLock := NewFileLock("/Users/songlihuang/temp/temp3")
	if fileLock.TryLock() != nil {
		fmt.Println("try lock fail...")
		return
	}
	defer fileLock.Unlock()
	fmt.Println("get lock success!")
	time.Sleep(3 * time.Second)
}

func TestFileLock_Lock(t *testing.T) {
	for i := 0; i < 10; i++ {
		go tryLockFunc()
		time.Sleep(2 * time.Second)
	}

	os.Getpid()

	time.Sleep(10 * time.Second)
}
