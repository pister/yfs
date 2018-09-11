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

// if there are more than one item, such as: item1, item2, item3 ...
// the order result is that: item1, item2, item3, ... , 'old items'
func (list *CopyOnWriteList) AddFirst(items ... interface{}) {
	itemsLen := len(items)
	if itemsLen == 0 {
		return
	}
	for {
		oldList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
		oldLen := len(oldList)
		newLen := oldLen + itemsLen
		newList := make([]interface{}, newLen, newLen)
		// the new item is at the first
		copy(newList, items)
		copy(newList[itemsLen:], oldList)
		newListPtr := unsafe.Pointer(&newList)
		if atomic.CompareAndSwapPointer(&list.innerList, list.innerList, newListPtr) {
			return
		}
	}
}

// if there are more than one item, such as: item1, item2, item3 ...
// the order result is that:  'old items', item1, item2, item3, ... ,
func (list *CopyOnWriteList) AddLast(items ... interface{}) {
	itemsLen := len(items)
	if itemsLen == 0 {
		return
	}
	for {
		oldList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
		oldLen := len(oldList)
		newLen := oldLen + itemsLen
		newList := make([]interface{}, newLen, newLen)
		// the new item is at the first
		copy(newList, oldList)
		copy(newList[oldLen:], items)
		newListPtr := unsafe.Pointer(&newList)
		if atomic.CompareAndSwapPointer(&list.innerList, list.innerList, newListPtr) {
			return
		}
	}
}


// it use '==' to check if they are equals, the function will all matches items.
func (list *CopyOnWriteList) Delete(items ... interface{}) {
	if len(items) == 0 {
		return
	}
	deletingItemsMap := make(map[interface{}]int)
	for _, item := range items {
		deletingItemsMap[item] = 1
	}
	for {
		oldList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
		oldLen := len(oldList)
		if oldLen == 0 {
			return
		}
		newLen := oldLen - len(items)
		if newLen < 0 {
			newLen = 0
		}
		newList := make([]interface{}, 0, newLen)
		for _, existItem := range oldList {
			_, deleting := deletingItemsMap[existItem]
			if !deleting {
				newList = append(newList, existItem)
			}
		}
		newListPtr := unsafe.Pointer(&newList)
		if atomic.CompareAndSwapPointer(&list.innerList, list.innerList, newListPtr) {
			return
		}
	}
}

func (list *CopyOnWriteList) Length() int {
	theList := *(*[]interface{})(atomic.LoadPointer(&list.innerList))
	return len(theList)
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
