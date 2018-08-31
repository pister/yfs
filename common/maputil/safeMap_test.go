package maputil

import (
	"testing"
	"fmt"
	"time"
	"sync"
)

func TestSafeMap1(t *testing.T) {
	m := NewSafeMap()
	m.Put("hello1", "v11")
	m.Put("hello2", "v22")
	m.Put("hello3", "v33")
	v, _ := m.Get("hello2")
	if v != "v22" {
		t.Fatal("error")
	}
	v, _ = m.Get("hello3")
	if v != "v33" {
		t.Fatal("error")
	}
	v, _ = m.Get("hello1")
	if v != "v11" {
		t.Fatal("error")
	}
	m.Foreach(func(key interface{}, value interface{}) (stop bool) {
		fmt.Println(key, "=>", value)
		return false
	})
}

func TestSafeMap2(t *testing.T) {
	m := NewSafeMap()
	m.Put(444, "v11")
	m.Put(555, "v22")
	m.Put(666, "v33")
	v, _ := m.Get(555)
	if v != "v22" {
		t.Fatal("error")
	}
	m.Foreach(func(key interface{}, value interface{}) (stop bool) {
		fmt.Println(key, "=>", value)
		return false
	})
}

func TestSafeMapMultiRoutines(t *testing.T) {
	m := NewSafeMap()
	for i := 0; i < 3; i++ {
		go func(i int) {
			for n := 0; n < 100; n++ {
				m.Put(fmt.Sprintf("key-%d-%d", i, n), fmt.Sprintf("value-%d-%d", i, n))
			}
		}(i)
	}

	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			for n := 0; n < 100; n++ {
				key := fmt.Sprintf("key-%d-%d", i, n)
				v, exist := m.Get(key)
				if !exist {
					fmt.Println(key, " not exist.")
				} else {
					fmt.Println(key, " => ", v)
				}
				time.Sleep(50 * time.Millisecond)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	m.Foreach(func(key interface{}, value interface{}) (stop bool) {
		fmt.Println(">>>", key, " = ", value)
		return false
	})
}