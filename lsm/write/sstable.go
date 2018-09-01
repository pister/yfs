package write

import (
	"os"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
)

type SSTableWriter struct {
	indexFile *os.File
	dataFile  *os.File
	position  uint32
}

const (
	dataMagicCode1 = 'D'
	dataMagicCode2 = 'T'
	indexMagicCode1 = 'I'
	indexMagicCode2 = 'N'
)

func NewSSTableWriter(path string, name string) (*SSTableWriter, error) {
	return nil, nil
}

func (writer *SSTableWriter) writeData(key []byte, data *Data) (uint32, error) {
	dataIndex := writer.position
	/*
	2 - bytes magic code
	1 - byte delete flag
	1 - byte reserved
	4 - bytes data sum
	8 - bytes ts
	4 - bytes key length
	4 - bytes data length
	bytes for key
	bytes for data
	*/
	headerAndKey := make([]byte, 24+len(key))
	headerAndKey[0] = dataMagicCode1
	headerAndKey[1] = dataMagicCode2
	headerAndKey[2] = data.deleted
	headerAndKey[3] = 0
	dataSum := hashutil.SumHash32(data.value)
	bytesutil.CopyUint32ToBytes(dataSum, headerAndKey, 4)
	bytesutil.CopyUint64ToBytes(data.ts, headerAndKey, 8)
	bytesutil.CopyUint32ToBytes(uint32(len(key)), headerAndKey, 16)
	bytesutil.CopyUint32ToBytes(uint32(len(data)), headerAndKey, 20)
	bytesutil.CopyDataToBytes(key, 0, headerAndKey, 24, len(key))
	_, err := writer.dataFile.Write(headerAndKey)
	if err != nil {
		return 0, nil
	}
	_, err = writer.dataFile.Write(data)
	if err != nil {
		return 0, nil
	}
	return dataIndex, nil
}


func (writer *SSTableWriter) WriteData(key []byte, data *Data) (uint32, error) {
	return writer.writeData(key, data)
}

func (writer *SSTableWriter) Commit() error {
	return nil
}
