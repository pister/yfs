package region

import (
	"testing"
	"fmt"
	"github.com/pister/yfs/naming"
)

func TestNameBlock1(t *testing.T) {
	nameBlock, err := OpenNameBlock(1, "/Users/songlihuang/temp/temp3/name_test", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer nameBlock.Close()
	name, err := nameBlock.Add(DataIndex{1, 211})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(name.ToString())
	di, exist, err := nameBlock.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Fatal("must be exists!")
	}
	fmt.Println(di)

}

func TestOpenNameBlock(t *testing.T) {
	nameBlock, err := OpenNameBlock(1, "/Users/songlihuang/temp/temp3/name_test", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer nameBlock.Close()
	name, err := naming.ParseNameFromString("AAAAhAAAAAEAAchi")
	if err != nil {
		t.Fatal(err)
	}
	di, exist, err := nameBlock.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Fatal("must be exists!")
	}
	fmt.Println(di)
}