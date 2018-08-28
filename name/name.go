package name

type Name struct {
	NameServerIndex uint16
	BlockIndex      uint16
	PositionIndex   uint16
	Reserved        uint16
}

func (name Name) ToInt64() uint64 {
	var val uint64 = 0
	val |= uint64(name.NameServerIndex)
	val <<= 16
	val |= uint64(name.BlockIndex)
	val <<= 16
	val |= uint64(name.PositionIndex)
	val <<= 16
	val |= uint64(name.Reserved)
	return val
}

func NewNameFromValue(val uint64) Name {
	var name Name
	name.Reserved = uint16(0xFFFF & val)
	name.PositionIndex = uint16(0xFFFF & (val >> 16))
	name.BlockIndex = uint16(0xFFFF & (val >> 32))
	name.NameServerIndex = uint16(0xFFFF & (val >> 48))
	return name
}
