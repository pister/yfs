package ioutil

import (
	"testing"
	"fmt"
	"time"
	"sync"
)

func TestNewConcurrentFileReader(t *testing.T) {
	reader, err := openAsConcurrentReadFile("readWriteFile_test.go", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for i := 0; i < 10; i++ {
		var n = i
		go func() {
			buf := make([]byte, 16, 16)
			v, err := reader.SeekAndReadData(int64(10+n), buf)
			fmt.Println("buf:", n, string(buf))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("buf: ", v, string(buf))
		}()
	}
	time.Sleep(10 * time.Second)
}

func TestOpenReadWriteFile(t *testing.T) {
	file, err := OpenReadWriteFile("/Users/songlihuang/temp/temp3/a2/a5",2)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	wg := sync.WaitGroup{}
	wg.Add(4)
	for n := 0; n < 2; n++ {
		n1 := n
		go func() {
			for i := 0; i < 100; i++ {
				_, err := file.Append([]byte(fmt.Sprintf("hello-%d-%d\n", n1,i)))
				if err != nil {
					fmt.Println(err)
				}
				time.Sleep(1 * time.Second)
			}
		}()
	}

	go func() {
		time.Sleep(4 * time.Second)
		file.UpdateByteAt(2, 'X')
	}()

	for i := 0; i < 4; i++ {
		seq := i
		go func() {
			buf := make([]byte, 10)
			for index := 0; index < 10; index++ {
				_, err := file.SeekAndReadData(int64(10 * seq), buf)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("seq-%d:%s", seq, string(buf))
				}
				time.Sleep(400 * time.Millisecond)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
