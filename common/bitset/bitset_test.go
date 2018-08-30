package bitset

import (
	"testing"
)

func assetPos(t *testing.T, bitset *BitSet, pos uint32) {
	if bitset.Get(pos) {
		t.Fatalf("fail for pos: %d", pos)
	}
	bitset.Set(pos, true)
	if !bitset.Get(pos) {
		t.Fatalf("fail for pos: %d", pos)
	}
}

func TestBitSet(t *testing.T) {
	bitset := NewBitSet(1027)
	assetPos(t, bitset, 0)
	assetPos(t, bitset, 1)
	assetPos(t, bitset, 7)
	assetPos(t, bitset, 8)
	assetPos(t, bitset, 9)
	assetPos(t, bitset, 1024)
	assetPos(t, bitset, 1025)
	assetPos(t, bitset, 1026)
}
