package maputil

// the bytes-tree-map, this map is not thread-safe.
//
// Elements are ordered by key in the map.
//
// Structure is not thread safe.
//
// Reference: http://en.wikipedia.org/wiki/Associative_array
// modify from https://github.com/golang-collections/tree

import (
	rbt "github.com/pister/yfs/common/maputil/redblacktree"
)

type TreeMap struct {
	tree *rbt.Tree
}

func NewTreeMap() *TreeMap {
	return &TreeMap{tree: rbt.NewTree()}
}

func (m *TreeMap) Put(key []byte, value interface{}) {
	m.tree.Put(key, value)
}

func (m *TreeMap) Get(key []byte) (value interface{}, found bool) {
	return m.tree.Get(key)
}

func (m *TreeMap) Delete(key []byte) {
	m.tree.Remove(key)
}

func (m *TreeMap) Length() int {
	return m.tree.Size()
}

func (m *TreeMap) Clear() {
	m.tree.Clear()
}

func (m *TreeMap) Min() (key []byte, value interface{}) {
	if node := m.tree.Left(); node != nil {
		return node.Key, node.Value
	}
	return nil, nil
}

func (m *TreeMap) Max() (key []byte, value interface{}) {
	if node := m.tree.Right(); node != nil {
		return node.Key, node.Value
	}
	return nil, nil
}

func (m *TreeMap) Foreach(callback func(key []byte, value interface{}) bool ) error {
	it := m.tree.Iterator()
	for it.Next() {
		if callback(it.Key(), it.Value()) {
			return nil
		}
	}
	return nil
}
