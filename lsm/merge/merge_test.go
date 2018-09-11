package merge

import (
	"testing"
	"fmt"
	"github.com/pister/yfs/lsm"
	"path/filepath"
	"os"
	"github.com/pister/yfs/lsm/base"
)

func TestSSTFileBlockReader(t *testing.T) {
	reader, err := OpenSstFileDataBlockReader("/Users/songlihuang/temp/temp3/lsm_test/sst_a_1536199573")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for i := 0; i < 10; i++ {
		d, err := reader.PeekNextData()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(string(d.key), string(d.value))
	}
	fmt.Println("xxxxxxx=========")
	for {
		d, err := reader.PopNextData()
		if err != nil {
			t.Fatal(err)
		}
		if d == nil {
			break
		}
		fmt.Println(string(d.key), string(d.value))
	}
}

func TestMergeFiles1(t *testing.T) {
	basePath := "/Users/songlihuang/temp/temp3/lsm_merge_test"
	lsm1, err := lsm.OpenLsm(basePath)
	if err != nil {
		t.Fatal(err)
	}
	defer  os.RemoveAll(basePath)
	lsm1.Put([]byte("name1"), []byte("value1"))
	lsm1.Put([]byte("name2"), []byte("value2"))
	lsm1.Put([]byte("name3"), []byte("value3"))
	lsm1.Flush()
	lsm1.Close()

	lsm1, err = lsm.OpenLsm(basePath)
	if err != nil {
		t.Fatal(err)
	}
	lsm1.Put([]byte("name3"), []byte("value3333"))
	lsm1.Delete([]byte("name1"))
	lsm1.Flush()
	lsm1.Close()

	sstFiles := make([]string, 0, 10)
	filepath.Walk(basePath, func(file string, info os.FileInfo, err error) error {
		_, name := filepath.Split(file)
		if base.SSTNamePattern.MatchString(name) {
			sstFiles = append(sstFiles, file)
		}
		return nil
	})

	CompactFiles(sstFiles, true)

	lsm2, err := lsm.OpenLsm("/Users/songlihuang/temp/temp3/lsm_merge_test")
	if err != nil {
		t.Fatal(err)
	}
	var value []byte
	for i := 1; i <= 4; i++ {
		value, _ = lsm2.Get([]byte(fmt.Sprintf("name%d", i)))
		fmt.Println(string(value))
	}
	lsm2.Close()
}
