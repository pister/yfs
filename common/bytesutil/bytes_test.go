package bytesutil

import (
	"testing"
	"fmt"
)

func TestCopyUint32ToBytes(t *testing.T) {
	bytes := make([]byte, 20, 20)
	var input uint32 = 123456789
	CopyUint32ToBytes(input, bytes, 12)
	v := GetUint32FromBytes(bytes, 12)
	if v != input {
		t.Fatal("fail")
	}
}

func TestCopyUint16ToBytes(t *testing.T) {
	bytes := make([]byte, 20, 20)
	var input uint16 = 12345
	CopyUint16ToBytes(input, bytes, 12)
	v := GetUint16FromBytes(bytes, 12)
	if v != input {
		t.Fatal("fail")
	}
}

func TestCopyDataToBytes(t *testing.T) {
	bytes := make([]byte, 20, 20)
	input := []byte("hello")
	CopyDataToBytes(input, 0, bytes, 10, len(input))
	dest := make([]byte, len(input), len(input))
	CopyDataToBytes(bytes, 10, dest, 0, len(input))
	if !IsDataEquals(dest, 0, input, 0, len(input)) {
		t.Fatal("fail")
	}
}


func Test1(t *testing.T) {
	bytes := []byte("12345678")
	fmt.Println(string(bytes[2:5]))
}