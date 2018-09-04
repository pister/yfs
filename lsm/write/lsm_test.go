package write

import (
	"testing"
	"fmt"
	"os"
	"github.com/pister/yfs/common/fileutil"
	"sync"
	"math/rand"
	"time"
	"github.com/pister/yfs/common/atomicutil"
)

func TestLsmPutAndGet(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/lsm_test"
	fileutil.MkDirs(tempDir)
	defer os.RemoveAll(tempDir)
	lsm, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		err = lsm.Put([]byte(fmt.Sprintf("name-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		data, err := lsm.Get([]byte(fmt.Sprintf("name-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != fmt.Sprintf("value-%d", i) {
			t.Fatal("value not match")
		}
	}
}

func TestLsmPutAndGetAndDelete(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/lsm_test"
	fileutil.MkDirs(tempDir)
	defer os.RemoveAll(tempDir)
	lsm, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		err = lsm.Put([]byte(fmt.Sprintf("name-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		data, err := lsm.Get([]byte(fmt.Sprintf("name-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != fmt.Sprintf("value-%d", i) {
			t.Fatal("value not match")
		}
	}
	if err := lsm.Delete([]byte("name-0")); err != nil {
		t.Fatal(err)
	}
	data, err := lsm.Get([]byte("name-0"))
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatal("delete fail")
	}
	lsm.Put([]byte("name-0"), []byte("value-0-00"))
	data, err = lsm.Get([]byte("name-0"))
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("get fail")
	}
	if string(data) != "value-0-00" {
		t.Fatal("value not match")
	}
}

func TestFlush(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/lsm_test"
	fileutil.MkDirs(tempDir)
	defer os.RemoveAll(tempDir)
	lsm, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	for i := 0; i < 10; i++ {
		err = lsm.Put([]byte(fmt.Sprintf("name-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		data, err := lsm.Get([]byte(fmt.Sprintf("name-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != fmt.Sprintf("value-%d", i) {
			t.Fatal("value not match")
		}
	}
	if err := lsm.Delete([]byte("name-0")); err != nil {
		t.Fatal(err)
	}
	data, err := lsm.Get([]byte("name-0"))
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatal("delete fail")
	}
	lsm.Put([]byte("name-1"), []byte("value-1-11"))
	data, err = lsm.Get([]byte("name-1"))
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("get fail")
	}
	if string(data) != "value-1-11" {
		t.Fatal("value not match")
	}
	err = lsm.Flush()
	if err != nil {
		t.Fatal(err)
	}
	data, err = lsm.Get([]byte("name-1"))
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		// 异步未完成
		t.Fatal("get fail")
	}
	if string(data) != "value-1-11" {
		t.Fatal("value not match")
	}
	err = lsm.Put([]byte("name-2-1"), []byte("value-2-1"))
	if err != nil {
		t.Fatal(err)
	}
	data, err = lsm.Get([]byte("name-2-1"))
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("get fail")
	}
	if string(data) != "value-2-1" {
		t.Fatal("value not match")
	}
	err = lsm.Flush()
	if err != nil {
		t.Fatal(err)
	}
	data, err = lsm.Get([]byte("name-1"))
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("get fail")
	}
	if string(data) != "value-1-11" {
		t.Fatal("value not match")
	}
	lsm.Put([]byte("name-1"), []byte("value-1-22"))
	data, err = lsm.Get([]byte("name-1"))
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("get fail")
	}
	if string(data) != "value-1-22" {
		t.Fatal("value not match")
	}
}

func TestMultiRoutine(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/lsm_test"
	fileutil.MkDirs(tempDir)
//	defer os.RemoveAll(tempDir)
	lsm, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	wg := sync.WaitGroup{}

	wg.Add(10)
	nameChan := make(chan string, 100)
	for x := 0; x < 10; x++ {
		go func(x int) {
			for i := 0; i < 100000; i++ {
				n := rand.Int31n(100000)
				name := fmt.Sprintf("name-%d-%d", x, n)
				value := fmt.Sprintf("value-%d-%d", x, n)
				lsm.Put([]byte(name), []byte(value))
				nameChan <- name
			}
			nameChan <- "<end>"
		}(x)
	}
	counter := atomicutil.NewAtomicUint64(0)
	for x := 0; x < 100; x++ {
		go func() {
			for {
				name := <-nameChan
				if name == "<end>" {
					wg.Done()
					continue
				}
				value, err := lsm.Get([]byte(name))
				if err != nil {
					t.Fatal(err)
				}
				if value == nil {
					t.Fatal("not found:", name)
				}
				counter.Increment()
				fmt.Println(name, "===>", string(value))
			}
		}()
	}

	wg.Wait()
	time.Sleep(1 * time.Second) // wait for some routine end...
	fmt.Println("=============count", counter.Get())
}
