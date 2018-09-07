package process

import (
	"testing"
	"fmt"
	"time"
)

func TestOpenLockerTryLock(t *testing.T) {
	locker := OpenLocker("/Users/songlihuang/temp/temp3/lock_test/yfs_lock")
	x := locker.TryLock()
	fmt.Println(x)
	if x {
		time.Sleep(10 * time.Second)

		locker.Unlock()
	}
}

func TestOpenLockerLock(t *testing.T) {
	locker := OpenLocker("/Users/songlihuang/temp/temp3/lock_test/yfs_lock")
	locker.Lock()
	fmt.Println("got lock!")
	time.Sleep(10 * time.Second)
	locker.Unlock()
}
