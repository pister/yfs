package listutil

import (
	"sync/atomic"
	"unsafe"
)

type CopyOnWriteList struct {
	innerList unsafe.Pointer //	innerList *[]interface{}
}

func NewCopyOnWriteList() *CopyOnWriteList {
	ret := new(CopyOnWriteList)
	newList := make([]interface{}, 0)
	ret.innerList = unsafe.Pointer(&newList)
	return ret
}

func NewCopyOnWriteListWithInitData(initData []interface{}) *CopyOnWriteList {
	ret := new(CopyOnWriteList)
	newList := make([]interface{}, len(initData))
	copy(newList, initData)
	ret.innerList = unsafe.Pointer(&newList)
	return ret
}

func (list *CopyOnWriteList) Add(item interface{}) {
	for {
		oldList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
		oldLen := len(oldList)
		newList := make([]interface{}, oldLen+1, oldLen+1)
		// the new item is at the first
		newList[0] = item
		copy(newList[1:], oldList)
		newListPtr := unsafe.Pointer(&newList)
		if atomic.CompareAndSwapPointer(&list.innerList, list.innerList, newListPtr) {
			return
		}
	}
}

func (list *CopyOnWriteList) Foreach(callback func(item interface{}) (bool, error)) error {
	theList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
	for _, item := range theList {
		stop, err := callback(item)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}
