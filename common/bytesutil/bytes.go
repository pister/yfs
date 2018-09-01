package bytesutil

import "encoding/binary"

func CopyUint16ToBytes(val uint16, dest []byte, pos int) {
	binary.BigEndian.PutUint16(dest[pos:], val)
}

func GetUint16FromBytes(dest []byte, pos int) uint16 {
	return binary.BigEndian.Uint16(dest[pos:])
}

func CopyUint32ToBytes(val uint32, dest []byte, pos int) {
	binary.BigEndian.PutUint32(dest[pos:], val)
}

func CopyUint64ToBytes(val uint64, dest []byte, pos int) {
	binary.BigEndian.PutUint64(dest[pos:], val)
}

func Uint32ToBytes(val uint32) []byte {
	dest := make([]byte, 4, 4)
	CopyUint32ToBytes(val, dest, 0)
	return dest
}

func GetUint32FromBytes(dest []byte, pos int) uint32 {
	return binary.BigEndian.Uint32(dest[pos:])
}

func GetUint64FromBytes(dest []byte, pos int) uint64 {
	return binary.BigEndian.Uint64(dest[pos:])
}

func CopyDataToBytes(src []byte, srcPos int, dest []byte, destPos int, len int) {
	copy(dest[destPos:destPos+len], src[srcPos:srcPos+len])
}

func IsDataEquals(src []byte, srcPos int, dest []byte, destPos int, len int) bool {
	for i := 0; i < len; i++ {
		if dest[destPos+i] != src[srcPos+i] {
			return false
		}
	}
	return true
}
