package lsm

import (
	"testing"
	"fmt"
	"os"
	"github.com/pister/yfs/common/fileutil"
)

func TestSSTableReader_GetByKey(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/sstable_test"
	fileutil.MkDirs(tempDir)
	defer os.RemoveAll(tempDir)
	lsm, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		lsm.Put([]byte(fmt.Sprintf("name-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	lsm.Delete([]byte("name-0"))

	err = lsm.Flush()
	if err != nil {
		t.Fatal(err)
	}

}
