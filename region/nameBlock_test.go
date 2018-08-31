package region

import (
	"testing"
	"fmt"
)

func TestNameBlock1(t *testing.T) {
	nameBlock, err := OpenNameBlock(1, "/Users/songlihuang/temp/temp3/name_test", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer nameBlock.Close()
	name, err := nameBlock.Add(DataIndex{1, 200})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(name)
	di, exist, err := nameBlock.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Fatal("must be exists!")
	}
	fmt.Println(di)
	err = nameBlock.Delete(name)
	if err != nil {
		t.Fatal(err)
	}
	di, exist, err = nameBlock.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatal("must not be exists!")
	}
	fmt.Println(di)

}
