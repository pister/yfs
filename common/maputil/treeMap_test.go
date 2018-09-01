package maputil

import (
	"testing"
	"fmt"
)

func Test1(t *testing.T) {
	m := NewTreeMap()
	m.Put([]byte("name20"), "222")
	m.Put([]byte("name21"), "2221")
	m.Put([]byte("name19"), "22219")
	m.Put([]byte("aaa"), "222aa")
	m.Put([]byte("ccc"), "222cc")
	m.Put([]byte("bbb"), "222bb")
	m.Foreach(func(key []byte, value interface{}) {
		fmt.Println(string(key), "=>", value.(string))
	})
}
