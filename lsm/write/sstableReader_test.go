package write

import (
	"testing"
	"fmt"
	"os"
	"github.com/pister/yfs/common/ioutil"
)



func TestSSTableReader_GetByKey(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/sstable_test"
	ioutil.MkDirs(tempDir)
	defer os.RemoveAll(tempDir)
	lsm, err := NewLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	err = lsm.Put([]byte("name1"), []byte("value111"))
	err = lsm.Put([]byte("name2"), []byte("value222"))
	err = lsm.Put([]byte("name3"), []byte("value333"))
	if err != nil {
		t.Fatal(err)
	}
	v, found := lsm.Get([]byte("name2"))
	if found {
		fmt.Println(string(v))
	} else {
		fmt.Println("not found")
	}
	fileName, err := lsm.FlushAndClose()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := OpenSSTableReader(fileName)
	if err != nil {
		t.Fatal(err)
	}
	data, err := reader.GetByKey([]byte("name2"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}
