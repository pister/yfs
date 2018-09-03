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
	newList := make([]interface{}, 0, 0)
	ret.innerList = unsafe.Pointer(&newList)
	return ret
}

func (list *CopyOnWriteList) Add(item interface{}) {
	for {
		oldList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
		oldLen := len(oldList)
		newList := make([]interface{}, oldLen, oldLen+1)
		copy(newList, oldList)
		newList = append(newList, item)
		newListPtr := unsafe.Pointer(&newList)
		if atomic.CompareAndSwapPointer(&list.innerList, list.innerList, newListPtr) {
			return
		}
	}
}

func (list *CopyOnWriteList) Foreach(callback func(item interface{})) {
	theList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
	for _, item := range theList {
		callback(item)
	}
}

