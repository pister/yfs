package write

import (
	"testing"
	"fmt"
)

func TestLsm(t *testing.T) {
	lsm, err := NewLsm("/Users/songlihuang/temp/temp3/lsm_test")
	if err != nil {
		t.Fatal(err)
	}

	err = lsm.Put([]byte("name1"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = lsm.Put([]byte("name3"), []byte("value3"))
	if err != nil {
		t.Fatal(err)
	}
	err = lsm.Delete([]byte("name1"))
	if err != nil {
		t.Fatal(err)
	}
	err = lsm.Put([]byte("name2"), []byte("value2"))
	if err != nil {
		t.Fatal(err)
	}
	err = lsm.Put([]byte("name1"), []byte("value11"))
	if err != nil {
		t.Fatal(err)
	}
	err = lsm.Delete([]byte("name3"))
	if err != nil {
		t.Fatal(err)
	}
	var v []byte
	var found bool
	v, found = lsm.Get([]byte("name1"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}
	v, found = lsm.Get([]byte("name2"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}
	v, found = lsm.Get([]byte("name3"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}

}

func Test11(t *testing.T) {
	lsm, err := NewLsm("/Users/songlihuang/temp/temp3/lsm_test")
	if err != nil {
		t.Fatal(err)
	}
	var v []byte
	var found bool
	v, found = lsm.Get([]byte("name1"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}
	v, found = lsm.Get([]byte("name2"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}
	v, found = lsm.Get([]byte("name3"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}
	err = lsm.Put([]byte("name3"), []byte("value33"))
	if err != nil {
		t.Fatal(err)
	}
	v, found = lsm.Get([]byte("name3"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}
}

func Test22(t *testing.T) {
	a := make([]int, 0, 5)
	for i := 0; i < 5; i++ {
		a = append(a, i)
	}
	fmt.Println(a)
	fmt.Println(a[0:2])
	fmt.Println(a[2:])

}
