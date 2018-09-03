package write

import (
	"testing"
	"fmt"
	"os"
	"os/user"
)

func TestSSTableReader_GetByKey(t *testing.T) {
	user, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	workDir := fmt.Sprintf("%s/temp/lsm_test", user.HomeDir)

	reader, err := OpenSSTableReader("/Users/songlihuang/temp/temp3/lsm_test/sst_a_1535957071")
	if err != nil {
		t.Fatal(err)
	}
	data, err := reader.GetByKey([]byte("name3"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}
