package maputil

import (
	"sync"
	"github.com/pister/yfs/common/hashutil"
	"fmt"
)

type HashFunc func(key interface{}) uint32

type SafeMap struct {
	dataShards  []map[interface{}]interface{}
	mutexShards []*sync.Mutex
	hashFunc    HashFunc
}

const (
	shardCount = 8
)

func defaultHashFunc(key interface{}) uint32 {
	switch key.(type) {
	case int:
		return uint32(key.(int))
	case uint:
		return uint32(key.(uint))
	case int8:
		return uint32(key.(int8))
	case uint8:
		return uint32(key.(uint8))
	case int16:
		return uint32(key.(int16))
	case uint16:
		return uint32(key.(uint16))
	case int32:
		return uint32(key.(int32))
	case uint32:
		return uint32(key.(uint32))
	case int64:
		return uint32(key.(int64))
	case uint64:
		return uint32(key.(uint64))
	case string:
		return hashutil.SumHash32([]byte(key.(string)))
	default:
		return hashutil.SumHash32([]byte(fmt.Sprint(key)))
	}
}

func NewSafeMap(hashFunc ...HashFunc) *SafeMap {
	sm := new(SafeMap)
	if len(hashFunc) > 0 {
		sm.hashFunc = hashFunc[0]
	} else {
		sm.hashFunc = defaultHashFunc
	}
	sm.dataShards = make([]map[interface{}]interface{}, 0, 8)
	sm.mutexShards = make([]*sync.Mutex, 0, 8)
	for i := 0; i < shardCount; i++ {
		sm.dataShards = append(sm.dataShards, make(map[interface{}]interface{}))
		sm.mutexShards = append(sm.mutexShards, new(sync.Mutex))
	}
	return sm
}

func (m *SafeMap) Length() int {
	nLen := len(m.dataShards)
	lenChan := make(chan int, nLen)
	for i, shard := range m.dataShards {
		mutex := m.mutexShards[i]
		shardT := shard
		go func() {
			mutex.Lock()
			defer mutex.Unlock()
			n := len(shardT)
			lenChan <- n
		}()
	}
	sum := 0
	for i := 0; i < nLen; i++ {
		sum += <- lenChan
	}
	return sum
}

func (m *SafeMap) hashForKey(key interface{}) uint32 {
	return m.hashFunc(key)
}

func (m *SafeMap) execute(key interface{}, value interface{}, callback func(shardMap map[interface{}]interface{}, key interface{}, value interface{}) interface{}) interface{} {
	h := m.hashForKey(key)
	index := h % uint32(len(m.dataShards))
	shardMap := m.dataShards[index]
	mutex := m.mutexShards[index]
	mutex.Lock()
	defer mutex.Unlock()

	return callback(shardMap, key, value)
}

func (m *SafeMap) Put(key interface{}, value interface{}) {
	m.execute(key, value, func(shardMap map[interface{}]interface{}, key interface{}, value interface{}) interface{} {
		shardMap[key] = value
		return nil
	})
}

type resultObject struct {
	value interface{}
	exist bool
}

func (m *SafeMap) Get(key interface{}) (value interface{}, exist bool) {
	result := m.execute(key, nil, func(shardMap map[interface{}]interface{}, key interface{}, value interface{}) interface{} {
		v, ok := shardMap[key]
		result := new(resultObject)
		result.value = v
		result.exist = ok
		return result
	}).(*resultObject)
	return result.value, result.exist
}

func (m *SafeMap) Delete(key interface{}) {
	m.execute(key, nil, func(shardMap map[interface{}]interface{}, key interface{}, value interface{}) interface{} {
		delete(shardMap, key)
		return nil
	})
}

func iteratorShard(mutex *sync.Mutex, shard map[interface{}]interface{}, callback func(key interface{}, value interface{}) (stop bool)) bool {
	mutex.Lock()
	defer mutex.Unlock()
	for key, value := range shard {
		if callback(key, value) {
			return true
		}
	}
	return false
}

func (m *SafeMap) Foreach(callback func(key interface{}, value interface{}) (stop bool)) {
	for i, shard := range m.dataShards {
		mutex := m.mutexShards[i]
		if iteratorShard(mutex, shard, callback) {
			return
		}
	}
}
