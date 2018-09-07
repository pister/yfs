package lsm

import (
	"io"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
	"fmt"
	"github.com/pister/yfs/lsm/base"
)

type actionType byte

const (
	actionTypePut    actionType = iota
	actionTypeDelete
)

const defaultVersion = 1

type Action struct {
	version     byte
	op          actionType
	sumKeyValue uint16
	ts          uint64
	value       []byte
	key         []byte
}

func ActionFromReader(reader io.Reader) (*Action, error) {
	headerBuf := make([]byte, 20)
	_, err := reader.Read(headerBuf)
	if err != nil {
		return nil, err
	}
	action := new(Action)
	action.version = headerBuf[0]
	action.op = actionType(headerBuf[1])
	action.sumKeyValue = bytesutil.GetUint16FromBytes(headerBuf, 2)
	action.ts = bytesutil.GetUint64FromBytes(headerBuf, 4)
	keyLen := bytesutil.GetUint32FromBytes(headerBuf, 12)
	valueLen := bytesutil.GetUint32FromBytes(headerBuf, 16)
	if keyLen > base.MaxKeyLen {
		return nil, fmt.Errorf("too big key length: %d", keyLen)
	}
	if valueLen > base.MaxValueLen {
		return nil, fmt.Errorf("too big value length: %d", valueLen)
	}
	keyValueDataBuf := make([]byte, keyLen + valueLen)
	_, err = reader.Read(keyValueDataBuf)
	if err != nil {
		return nil, err
	}
	sumValue := hashutil.SumHash16(keyValueDataBuf)
	if sumValue != action.sumKeyValue {
		return nil, fmt.Errorf("wal sum value not match")
	}
	action.key = keyValueDataBuf[0:keyLen]
	action.value = keyValueDataBuf[keyLen:]
	return action, nil
}

func (action *Action) WriteTo(writer io.Writer) (int, error) {
	writtenLen := 20+len(action.key)+len(action.value)
	buf := make([]byte, writtenLen)
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
	bytesutil.CopyDataToBytes(action.value, 0, buf, 20+keyLen, valueLen)
	sumValue := hashutil.SumHash16(buf[20:])
	bytesutil.CopyUint16ToBytes(sumValue, buf, 2)
	_, err := writer.Write(buf)
	return writtenLen, err
}
