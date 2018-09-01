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

type Map struct {
	tree *rbt.Tree
}

func NewTreeMap() *Map {
	return &Map{tree: rbt.NewTree()}
}

func (m *Map) Put(key []byte, value []byte) {
	m.tree.Put(key, value)
}

func (m *Map) Get(key []byte) (value []byte, found bool) {
	return m.tree.Get(key)
}

func (m *Map) Delete(key []byte) {
	m.tree.Remove(key)
}

func (m *Map) Length() int {
	return m.tree.Size()
}

func (m *Map) Clear() {
	m.tree.Clear()
}

func (m *Map) Min() (key []byte, value []byte) {
	if node := m.tree.Left(); node != nil {
		return node.Key, node.Value
	}
	return nil, nil
}

func (m *Map) Max() (key []byte, value []byte) {
	if node := m.tree.Right(); node != nil {
		return node.Key, node.Value
	}
	return nil, nil
}

func (m *Map) Foreach(callback func(key []byte, value []byte)) {
	it := m.tree.Iterator()
	for it.Next() {
		callback(it.Key(), it.Value())
	}
}

