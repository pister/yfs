package region

import (
	"testing"
	"path/filepath"
	"os"
	"path"
	"fmt"
)

func TestWalk(t *testing.T) {
	filepath.Walk("/Users/songlihuang/temp/temp3/block_test", func(file string, info os.FileInfo, err error) error {
		dir, name := path.Split(file)
		fmt.Println(dir, name)
		return nil
	})
}
