package bloom

import (
	"testing"
	"fmt"
	"time"
	"sync"
)

func TestSafeBloomFilter(t *testing.T) {
	filter := NewSafeBloomFilter(1024 * 100)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for n := 0; n < 1000; n++ {
				filter.Add([]byte(fmt.Sprintf("hello-%d-%d", i, n)))
			}
		}(i)
	}
	wg := sync.WaitGroup{}
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			for n := 0; n < 1000; n++ {
				fmt.Println(filter.Hit([]byte(fmt.Sprintf("hello-%d-%d", i, n))))
			}
			time.Sleep(100 * time.Millisecond)
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("new full hit!!!>>>>>>>>>>>>>>>>")
	wg = sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for n := 0; n < 1000; n++ {
				fmt.Println(filter.Hit([]byte(fmt.Sprintf("hello-%d-%d", i, n))))
			}
			time.Sleep(100 * time.Millisecond)
			wg.Done()
		}(i)
	}
	wg.Wait()
}