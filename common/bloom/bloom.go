package bloom

import (
	"github.com/pister/yfs/common/bitset"
	"github.com/pister/yfs/common/hashutil/murmur3"
)

type Filter interface {
	Add(data []byte)
	Hit(data []byte) bool
}

type hashMaker struct {
	hashers []murmur3.Hash128
}

func NewHashMaker() *hashMaker {
	hashers := make([]murmur3.Hash128, 2)
	hashers[0] = murmur3.New128WithSeed(7)
	hashers[1] = murmur3.New128WithSeed(53)
	return &hashMaker{hashers: hashers}
}

func (maker *hashMaker) hash(data []byte) []uint32 {
	hashValues := make([]uint32, 0, 8)
	for _, hasher := range maker.hashers {
		hasher.Reset()
		hasher.Write(data)
		v1, v2 := hasher.Sum128()
		v10 := uint32(v1)
		v11 := uint32(v1 >> 32)
		v20 := uint32(v2)
		v21 := uint32(v2 >> 32)
		hashValues = append(hashValues, v10, v11, v20, v21)
	}
	return hashValues
}

type UnsafeBloomFilter struct {
	bits          *bitset.BitSet
	hashMakerChan chan *hashMaker
}

func NewUnsafeBloomFilter(bitLength uint32) Filter {
	bf := new(UnsafeBloomFilter)
	bf.hashMakerChan = make(chan *hashMaker, 4)
	for i := 0; i < 4; i++ {
		bf.hashMakerChan <- NewHashMaker()
	}
	bf.bits = bitset.NewBitSet(bitLength)
	return bf
}

func (filter *UnsafeBloomFilter) Add(data []byte) {
	bitLength := filter.bits.Length()
	hashValues := filter.hash(data)
	for _, value := range hashValues {
		filter.bits.Set(value%bitLength, true)
	}
}

func (filter *UnsafeBloomFilter) Hit(data []byte) bool {
	bitLength := filter.bits.Length()
	hashValues := filter.hash(data)
	for _, value := range hashValues {
		if !filter.bits.Get(value%bitLength) {
			return false
		}
	}
	return true
}

func (filter *UnsafeBloomFilter) hash(data []byte) []uint32 {
	select {
	case maker := <-filter.hashMakerChan:
		values := maker.hash(data)
		filter.hashMakerChan <- maker
		return values
	default:
		newMaker := NewHashMaker()
		return newMaker.hash(data)
	}
}
