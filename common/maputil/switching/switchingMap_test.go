package switching

import (
	"testing"
	"fmt"
)

func TestSwitchMap(t *testing.T) {
	m := NewSwitchingMap()
	m.Put([]byte("name-1"), "value-1")
	m.Put([]byte("name-2"), "value-2")
	m.SwitchNew()
	m.Put([]byte("name-3"), "value-3")
	v, found := m.Get([]byte("name-1"))
	if !found {
		t.Fatal("err")
	}
	if v != "value-1" {
		t.Fatal("err")
	}
	v, found = m.Get([]byte("name-3"))
	if !found {
		t.Fatal("err")
	}
	if v != "value-3" {
		t.Fatal("err")
	}
	m.ForeachMain(func(key []byte, value interface{}) bool {
		fmt.Println(string(key), "===>", value)
		return false
	})
	m.CleanSwitch()
	v, found = m.Get([]byte("name-1"))
	if found {
		t.Fatal("err")
	}
	v, found = m.Get([]byte("name-3"))
	if !found {
		t.Fatal("err")
	}
	if v != "value-3" {
		t.Fatal("err")
	}
}
