package region

import (
	"testing"
	"fmt"
	"time"
)

func TestNewConcurrentFileReader(t *testing.T) {
	reader, err := OpenAsConcurrentReadFile("file_test.go", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for i := 0; i < 10; i++ {
		var n = i
		go func() {
			buf := make([]byte, 16, 16)
			v, err := reader.SeekAndRead(int64(10+n), buf)
			fmt.Println("buf:", n, string(buf))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("buf: ", v, string(buf))
		}()
	}
	time.Sleep(10 * time.Second)
}
