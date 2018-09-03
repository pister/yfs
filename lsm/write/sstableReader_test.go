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
	for i := 0; i < 1000; i++ {
		lsm.Put([]byte(fmt.Sprintf("name-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	fileName, err := lsm.FlushAndClose()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := OpenSSTableReader(fileName)
	if err != nil {
		t.Fatal(err)
	}
	for i := 500; i < 1100 ; i++ {
		data, err := reader.GetByKey([]byte(fmt.Sprintf("name-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if data == nil {
			fmt.Println(fmt.Sprintf("name-%d", i), "not found")
			continue
		}

		if data.deleted == deleted {
			fmt.Println("data is deleted")
		} else {
			fmt.Println(fmt.Sprintf("name-%d", i) ,"=======>", string(data.value))
		}
	}

}
