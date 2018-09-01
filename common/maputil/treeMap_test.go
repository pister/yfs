package maputil

import (
	"testing"
	"fmt"
)

func Test1(t *testing.T) {
	m := NewTreeMap()
	m.Put([]byte("name20"), []byte("222"))
	m.Put([]byte("name21"), []byte("2221"))
	m.Put([]byte("name19"), []byte("22219"))
	m.Put([]byte("aaa"), []byte("222aa"))
	m.Put([]byte("ccc"), []byte("222cc"))
	m.Put([]byte("bbb"), []byte("222bb"))
	m.Foreach(func(key []byte, value []byte) {
		fmt.Println(string(key), "=>", string(value))
	})
}
