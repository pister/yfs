package lockutil

import (
	"testing"
	"fmt"
	"sync"
)

func TestNewTryLocker(t *testing.T) {
	locker := NewTryLocker()
	wg := sync.WaitGroup{}
	wg.Add(300)
	count := 0
	for i := 0; i < 300; i++ {
		go func() {
			defer wg.Done()
			if !locker.TryLock() {
				fmt.Println("not get lock")
				return
			}
			defer locker.UnLock()
			count += 1
			fmt.Println(count)

		}()
	}
	wg.Wait()
}
