package merge

import (
	"testing"
	"fmt"
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
	for  {
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