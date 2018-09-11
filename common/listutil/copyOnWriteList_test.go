package listutil

import (
	"testing"
	"fmt"
)

func TestCopyOnWriteList_Add(t *testing.T) {
	list := NewCopyOnWriteList()
	list.AddFirst("hello")
	list.AddFirst("world")
	list.AddFirst("Jack")
	list.Foreach(func(item interface{}) (bool, error) {
		s := item.(string)
		fmt.Println(s)
		return false, nil
	})
	list.AddFirst("item1", "item2", "item3")
	fmt.Println("after add first ====================>>>>>>>>>>>>>>>>")
	list.Foreach(func(item interface{}) (bool, error) {
		s := item.(string)
		fmt.Println(s)
		return false, nil
	})
	list.AddLast("item11", "item21", "item31")
	fmt.Println("after add last ====================>>>>>>>>>>>>>>>>")
	list.Foreach(func(item interface{}) (bool, error) {
		s := item.(string)
		fmt.Println(s)
		return false, nil
	})
}

func TestCopyOnWriteList_Delete(t *testing.T) {
	item2 := "item2"
	jack := "Jack"

	list := NewCopyOnWriteList()
	list.AddFirst("hello")
	list.AddFirst("world")
	list.AddFirst(jack)
	list.Foreach(func(item interface{}) (bool, error) {
		s := item.(string)
		fmt.Println(s)
		return false, nil
	})

	list.AddFirst("item1", item2, "item3")
	fmt.Println("after add ====================>>>>>>>>>>>>>>>>")
	list.Foreach(func(item interface{}) (bool, error) {
		s := item.(string)
		fmt.Println(s)
		return false, nil
	})
	list.Delete(item2, jack, "item3")
	fmt.Println("after delete ====================>>>>>>>>>>>>>>>>")
	list.Foreach(func(item interface{}) (bool, error) {
		s := item.(string)
		fmt.Println(s)
		return false, nil
	})
}
