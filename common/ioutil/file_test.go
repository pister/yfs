package ioutil

import (
	"testing"
	"fmt"
	"os"
)

func TestMkDirs(t *testing.T) {
	dir := "/Users/songlihuang/temp/temp3/a2/a4"
	err := MkDirs(dir)
	fmt.Println(err)
}

func TestWriteAndRead(t *testing.T) {
	path := "/Users/songlihuang/temp/temp3/1.txt"
	writer, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	_, err = writer.WriteString("hello world")
	if err != nil {
		t.Fatal(err)
	}
	c := make([]byte, 4, 4)
	_, err = reader.Read(c)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(c))
}

