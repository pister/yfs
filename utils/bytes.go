package utils

func CopyUint16ToBytes(val uint16, dest []byte, pos int) {
	// Big-Endian
	dest[pos] = byte(0xFF & (val >> 8))
	dest[pos+1] = byte(0xFF & val)
}

func GetUint16FromBytes(dest []byte, pos int) uint16 {
	val := uint16(dest[pos])
	val <<= 8
	val |= uint16(dest[pos+1])
	return val
}

func CopyUint32ToBytes(val uint32, dest []byte, pos int) {
	// Big-Endian
	dest[pos] = byte(0xFF & (val >> 24))
	dest[pos+1] = byte(0xFF & (val >> 16))
	dest[pos+2] = byte(0xFF & (val >> 8))
	dest[pos+3] = byte(0xFF & val)
}

func Uint32ToBytes(val uint32) []byte {
	dest := make([]byte, 4, 4)
	CopyUint32ToBytes(val, dest, 0)
	return dest
}

func GetUint32FromBytes(dest []byte, pos int) uint32 {
	val := uint32(dest[pos])
	val <<= 8
	val |= uint32(dest[pos+1])
	val <<= 8
	val |= uint32(dest[pos+2])
	val <<= 8
	val |= uint32(dest[pos+3])
	return val
}

func CopyDataToBytes(src []byte, srcPos int, dest []byte, destPos int, len int) {
	for i := 0; i < len; i++ {
		dest[destPos+i] = src[srcPos+i]
	}
}

func IsDataEquals(src []byte, srcPos int, dest []byte, destPos int, len int) bool {
	for i := 0; i < len; i++ {
		if dest[destPos+i] != src[srcPos+i] {
			return false
		}
	}
	return true
}
