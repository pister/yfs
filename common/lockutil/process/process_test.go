package process

import (
	"testing"
	"fmt"
	"time"
)

func TestOpenLockerTryLock(t *testing.T) {
	locker1 := OpenLocker("/Users/songlihuang/temp/temp3/lock_test/yfs_lock1")
	x1 := locker1.TryLock()
	fmt.Println("x1:", x1)

	locker2 := OpenLocker("/Users/songlihuang/temp/temp3/lock_test/yfs_lock2")
	x2 := locker2.TryLock()
	fmt.Println("x2:", x2)

	if x1 {
		time.Sleep(30 * time.Second)
		locker1.Unlock()
	}
	if x2 {
		time.Sleep(30 * time.Second)
		locker1.Unlock()
	}
}

func TestOpenLockerLock(t *testing.T) {
	locker := OpenLocker("/Users/songlihuang/temp/temp3/lock_test/yfs_lock")
	locker.Lock()
	fmt.Println("got lock!")
	time.Sleep(10 * time.Second)
	locker.Unlock()
}
