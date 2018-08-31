package region

import (
	"testing"
	"fmt"
	"sync"
)

func TestOpenBlockStore(t *testing.T) {
	blockStore, err := OpenDataBlock("/Users/songlihuang/temp/temp3/block_test", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	di, err := blockStore.Add([]byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(di)
	data, err := blockStore.Get(di)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(data))
}

type finishDataIndex struct {
	di     DataIndex
	finish bool
}

func TestOpenBlockStoreMultiRoutine(t *testing.T) {
	blockStore, err := OpenDataBlock("/Users/songlihuang/temp/temp3/block_test", 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	writerRoutines := 10
	wg := sync.WaitGroup{}
	wg.Add(writerRoutines)
	diChan := make(chan finishDataIndex, 100)
	for i := 0; i < writerRoutines; i++ {
		go func(i int) {
			defer func() {
				diChan <- finishDataIndex{DataIndex{}, true}
			}()
			for n := 0; n < 4000 * 1000; n++ {
				di, err := blockStore.Add([]byte(fmt.Sprintf("hello这个是一个中文测试，你要么也来试试-%d-%d", i, n)))
				if err != nil {
					t.Fatal(err)
					break
				}
				diChan <- finishDataIndex{di, false}
			}
		}(i)
	}

	for i := 0; i < 3; i++ {
		go func() {
			for {
				finishDataIndex := <-diChan
				if finishDataIndex.finish {
					fmt.Println("finished.")
					wg.Done()
					continue
				}
				data, err := blockStore.Get(finishDataIndex.di)
				if err != nil {
					t.Fatal(err)
				}
				fmt.Println(string(data))
			}
		}()
	}
	wg.Wait()
}


func TestBlockStore_Delete(t *testing.T) {
	blockStore, err := OpenDataBlock("/Users/songlihuang/temp/temp3/block_test", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	di, err := blockStore.Add([]byte("tobe delete data"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(di)
	data, err := blockStore.Get(di)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(data))
	err = blockStore.deleteData(di)
	if err != nil {
		t.Fatal(err)
	}
	data, err = blockStore.Get(di)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data)
}
