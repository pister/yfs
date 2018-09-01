package region

import (
	"testing"
	"path/filepath"
	"os"
	"path"
	"fmt"
	"github.com/pister/yfs/naming"
)

func TestWalk(t *testing.T) {
	filepath.Walk("/Users/songlihuang/temp/temp3/block_test", func(file string, info os.FileInfo, err error) error {
		dir, name := path.Split(file)
		fmt.Println(dir, name)
		return nil
	})
}


func TestOpenRegion(t *testing.T) {
	region, err := OpenRegion(1, "/Users/songlihuang/temp/temp3/region_test")
	if err != nil {
		t.Fatal(err)
	}
	defer region.Close()
	name, err := region.Add([]byte("hello test233"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(name.String())
	data, err := region.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(data))

}

func TestOpenRegion2(t *testing.T) {
	region, err := OpenRegion(1, "/Users/songlihuang/temp/temp3/region_test")
	if err != nil {
		t.Fatal(err)
	}
	defer region.Close()
	name, err := naming.ParseNameFromString("AAAAGAAAAAEAAW4O")
	if err != nil {
		t.Fatal(err)
	}
	data, err := region.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(data))
}