package bitset

//
// pos:       ...43210
//       		 |||||
// data: xxxxxxxxxxxxx
//
type BitSet struct {
	length  uint32
	bitData []byte
}

func NewBitSet(bitLength uint32) *BitSet {
	bs := new(BitSet)
	bs.length = bitLength
	byteSize := (bitLength + 7) / 8
	bitData := make([]byte, byteSize)
	bs.bitData = bitData
	return bs
}

func (bs *BitSet) Length() uint32 {
	return bs.length
}

func (bs *BitSet) Set(pos uint32, set bool) {
	if pos >= bs.length {
		return
	}
	byteIndex := pos >> 3 // equals: pos/8
	bitIndex := pos % 8
	if set {
		bs.bitData[byteIndex] |= byte(1 << bitIndex)
	} else {
		bs.bitData[byteIndex] &= ^byte(1 << bitIndex)
	}
}

func (bs *BitSet) Get(pos uint32) bool {
	if pos >= bs.length {
		return false
	}
	byteIndex := pos >> 3 // equals: pos/8
	bitIndex := pos % 8
	v := byte(1 << bitIndex)
	return (bs.bitData[byteIndex] & v) == v
}