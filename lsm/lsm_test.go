package lsm

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
	//defer os.RemoveAll(tempDir)
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
	//defer os.RemoveAll(tempDir)
	lsm, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	for i := 0; i < 100; i++ {
		err = lsm.Put([]byte(fmt.Sprintf("name-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
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
	lsm2, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm2.Close()
	data, err = lsm2.Get([]byte("name-1"))
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

	wg.Add(43)
	for x := 0; x < 10; x++ {
		go func(x int) {
			for i := 0; i < 1000000; i++ {
				n := rand.Int31n(100000)
				name := fmt.Sprintf("name-%d-%d", x, n)
				value := fmt.Sprintf("value-%d-%d", x, n)
				lsm.Put([]byte(name), []byte(value))
				time.Sleep(time.Duration(rand.Int31n(10)) * time.Millisecond)
			}
			wg.Done()
		}(x)
	}
	hit := atomicutil.NewAtomicUint64(0)
	notHit := atomicutil.NewAtomicUint64(0)
	total := atomicutil.NewAtomicUint64(0)
	for x := 0; x < 3; x++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 1000000; i++ {
				total.Increment()
				y := rand.Int31n(40)
				n := rand.Int31n(100000)
				name := fmt.Sprintf("name-%d-%d", y, n)
				value, err := lsm.Get([]byte(name))
				if err != nil {
					t.Fatal(err)
				}
				if value == nil {
					notHit.Increment()
				} else {
					if string(value) == fmt.Sprintf("value-%d-%d", y, n) {
						hit.Increment()
					} else {
						notHit.Increment()
						t.Fatal("error for not match!!!!")
					}
				}
				time.Sleep(30 * time.Millisecond)
			}

		}()
	}

	go func() {
		for {
			time.Sleep(1 * time.Second) // wait for some routine end...
			a := hit.Get()
			b := notHit.Get()
			fmt.Println("=============hit", hit.Get(), "not hit:", notHit.Get(), " total:", total.Get(), "hit rate:", float64(a)/(float64(a)+float64(b)))
		}
	}()

	wg.Wait()

}

func TestMultiRoutineRead(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/lsm_test"
	fileutil.MkDirs(tempDir)
	//defer os.RemoveAll(tempDir)
	lsm, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	wg := sync.WaitGroup{}

	wg.Add(3)
	hit := atomicutil.NewAtomicUint64(0)
	notHit := atomicutil.NewAtomicUint64(0)
	total := atomicutil.NewAtomicUint64(0)
	for x := 0; x < 2; x++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 1000000; i++ {
				total.Increment()
				y := rand.Int31n(40)
				n := rand.Int31n(100000)
				name := fmt.Sprintf("name-%d-%d", y, n)
				value, err := lsm.Get([]byte(name))
				if err != nil {
					t.Fatal(err)
				}
				if value == nil {
					notHit.Increment()
				} else {
					if string(value) == fmt.Sprintf("value-%d-%d", y, n) {
						hit.Increment()
					} else {
						notHit.Increment()
						t.Fatal("error for not match!!!!")
					}
				}
			}
		}()
	}

	go func() {
		for {
			time.Sleep(1 * time.Second) // wait for some routine end...
			a := hit.Get()
			b := notHit.Get()
			fmt.Println("=============hit", hit.Get(), "not hit:", notHit.Get(), " total:", total.Get(), "hit rate:", float64(a)/(float64(a)+float64(b)))
		}
	}()

	wg.Wait()

}

func TestOpenLsm(t *testing.T) {
	tempDir := "/Users/songlihuang/temp/temp3/lsm_test"
	fileutil.MkDirs(tempDir)
	//	defer os.RemoveAll(tempDir)
	lsm1, err := OpenLsm(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm1.Close()

	t1 := time.Now().UnixNano() / 1000000
	value, err := lsm1.Get([]byte("name-9-99949"))
	t2 := time.Now().UnixNano() / 1000000
	fmt.Println(t2 - t1)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		t.Fatal("not found")
	} else {
		fmt.Println(string(value))
	}
}
