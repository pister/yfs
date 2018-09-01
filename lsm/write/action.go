package write

import (
	"io"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
)

type actionType uint8

const (
	actionTypePut    actionType = iota
	actionTypeDelete
)

const defaultVersion = 1

type Action struct {
	version     uint8
	op          actionType
	sumKeyValue uint16
	ts          uint64
	value       []byte
	key         []byte
}

func (action *Action) WriteTo(writer io.Writer) error {
	buf := make([]byte, 20+len(action.key)+len(action.key))
	buf[0] = action.version
	buf[1] = byte(action.op)
	// buf[2 ... 4) = sumKeyValue 2bytes
	// see the bottom
	// buf[4 ...12) ts in 8bytes
	bytesutil.CopyUint64ToBytes(action.ts, buf, 4)
	// buf[12 ...16) key length in 4bytes
	keyLen := len(action.key)
	valueLen := len(action.value)
	bytesutil.CopyUint32ToBytes(uint32(keyLen), buf, 12)
	// buf[16 ...20) value length in 4bytes
	bytesutil.CopyUint32ToBytes(uint32(valueLen), buf, 16)
	bytesutil.CopyDataToBytes(action.key, 0, buf, 20, keyLen)
	bytesutil.CopyDataToBytes(action.value, 0, buf, 20 + keyLen, valueLen)
	sumValue := hashutil.SumHash16(buf[20: keyLen + valueLen])
	bytesutil.CopyUint16ToBytes(sumValue, buf, 2)
	_, err := writer.Write(buf)
	return err
}
