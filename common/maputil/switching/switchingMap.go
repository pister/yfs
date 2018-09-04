package switching

import (
	"unsafe"
	"github.com/pister/yfs/common/maputil"
	"sync/atomic"
)

type SwitchingMap struct {
	mainPtr      unsafe.Pointer
	switchingPtr unsafe.Pointer
}

func NewSwitchingMapWithInitData(initData *maputil.SafeTreeMap) *SwitchingMap {
	m := new(SwitchingMap)
	atomic.StorePointer(&m.mainPtr, unsafe.Pointer(initData))
	return m
}

func NewSwitchingMap() *SwitchingMap {
	m := new(SwitchingMap)
	atomic.StorePointer(&m.mainPtr, unsafe.Pointer(maputil.NewSafeTreeMap()))
	return m
}

func (sm *SwitchingMap) getMain() *maputil.SafeTreeMap {
	return (*maputil.SafeTreeMap)(atomic.LoadPointer(&sm.mainPtr))
}

func (sm *SwitchingMap) getSwitching() *maputil.SafeTreeMap {
	return (*maputil.SafeTreeMap)(atomic.LoadPointer(&sm.switchingPtr))
}

func (sm *SwitchingMap) SwitchNew() *maputil.SafeTreeMap  {
	oldMain := sm.getMain()
	atomic.StorePointer(&sm.switchingPtr, unsafe.Pointer(oldMain))
	atomic.StorePointer(&sm.mainPtr, unsafe.Pointer(maputil.NewSafeTreeMap()))
	return oldMain
}

func (sm *SwitchingMap) CleanSwitch() {
	atomic.StorePointer(&sm.switchingPtr, nil)
}

func (sm *SwitchingMap) MergeToMain() {
	sw := sm.getSwitching()
	if sw == nil {
		return
	}
	main := sm.getMain()
	main.Foreach(func(key []byte, value interface{}) bool {
		sw.Put(key, value)
		return false
	})
	atomic.StorePointer(&sm.mainPtr, unsafe.Pointer(sw))
	atomic.StorePointer(&sm.switchingPtr, nil)
}


func (sm *SwitchingMap) Put(key []byte, value interface{}) {
	sm.getMain().Put(key, value)
}

func (sm *SwitchingMap) Get(key []byte) (interface{}, bool) {
	v, found := sm.getMain().Get(key)
	if found {
		return v, true
	}
	w := sm.getSwitching()
	if w == nil {
		return nil, false
	}
	return w.Get(key)
}

func (sm *SwitchingMap) ForeachMain(callback func(key []byte, value interface{}) bool) {
	sm.getMain().Foreach(callback)
}
