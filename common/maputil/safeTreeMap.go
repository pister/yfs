package maputil

import "sync"

// thread-safe tree map

type SafeTreeMap struct {
	treeMap *TreeMap
	mutex   sync.Mutex
}

func (m *SafeTreeMap) SafeOperate(callback func(unsafeMap *TreeMap) (interface{}, error)) (interface{}, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return callback(m.treeMap)
}

func (m *SafeTreeMap) Put(key []byte, value interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.treeMap.Put(key, value)
}

func (m *SafeTreeMap) Get(key []byte) (value interface{}, found bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.treeMap.Get(key)
}

func (m *SafeTreeMap) Delete(key []byte) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.treeMap.Delete(key)
}

func (m *SafeTreeMap) Length() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.treeMap.Length()
}

func (m *SafeTreeMap) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.treeMap.Clear()
}

func (m *SafeTreeMap) Min() (key []byte, value interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.treeMap.Min()
}

func (m *SafeTreeMap) Max() (key []byte, value interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.treeMap.Max()

}

func (m *SafeTreeMap) Foreach(callback func(key []byte, value interface{}) bool ) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.treeMap.Foreach(callback)
	return nil
}

func NewSafeTreeMap() *SafeTreeMap {
	m := new(SafeTreeMap)
	m.treeMap = NewTreeMap()
	return m
}
