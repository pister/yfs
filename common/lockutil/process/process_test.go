package process

import (
	"testing"
	"fmt"
	"time"
	"sync"
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
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i:= 0; i < 100; i++ {
		go func(i int) {
			locker1 := OpenLocker("/Users/songlihuang/temp/temp3/lock_test/yfs_lock0")
			x1 := locker1.TryLock()
			fmt.Println("x-", i, x1)
			if x1 {
				time.Sleep(30 * time.Second)
				locker1.Unlock()
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}
