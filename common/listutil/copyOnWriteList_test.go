package listutil

import (
	"testing"
	"fmt"
)

func TestCopyOnWriteList_Add(t *testing.T) {
	list := NewCopyOnWriteList()
	list.Add("hello")
	list.Add("world")
	list.Add("Jack")
	list.Foreach(func(item interface{}) (bool, error) {
		s := item.(string)
		fmt.Println(s)
	})
}
