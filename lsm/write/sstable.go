package write

import (
	"os"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
)

type SSTableWriter struct {
	file     *os.File
	position uint32
}

const (
	dataMagicCode1     = 'D'
	dataMagicCode2     = 'T'
	keyMagicCode1      = 'K'
	keyMagicCode2      = 'Y'
	keyIndexMagicCode1 = 'I'
	keyIndexMagicCode2 = 'N'
	footerMagicCode1   = 'F'
	footerMagicCode2   = 'T'
	blockTypeData      = 1
	blockTypeKey       = 2
	blockTypeKeyIndex  = 3
	blockTypeFooter    = 4
)

/*
the sstable data format:
block-data-0
block-data-1
block-data-2
...
block-data-N
data-index-0    <- data-index-start-position
data-index-1
data-index-2
....
key-data-index-N
key-index-0			<- key-index-start-position
key-index-1
key-index-2
...
key-index-N
footer:data-index-start-position,key-index-start-position
 */
func NewSSTableWriter(path string, name string) (*SSTableWriter, error) {
	return nil, nil
}

func (writer *SSTableWriter) write(buf []byte) (uint32, error) {
	position := writer.position
	_, err := writer.file.Write(buf)
	if err != nil {
		return 0, err
	}
	writer.position += uint32(len(buf))
	return position, nil
}

func (writer *SSTableWriter) WriteFooter(dataIndexStartPosition uint32, keyIndexStartPosition uint32) error {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes key-Index
	4 - bytes key-index-start-position-Index
	*/
	buf := make([]byte, 8)
	buf[0] = footerMagicCode1
	buf[1] = footerMagicCode2
	buf[2] = 0
	buf[3] = blockTypeFooter
	bytesutil.CopyUint32ToBytes(dataIndexStartPosition, buf, 4)
	bytesutil.CopyUint32ToBytes(keyIndexStartPosition, buf, 8)
	_, err := writer.write(buf)
	return err
}

func (writer *SSTableWriter) writeKeyIndex(keyIndex uint32) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes key-Index
	*/
	buf := make([]byte, 8)
	buf[0] = keyIndexMagicCode1
	buf[1] = keyIndexMagicCode2
	buf[2] = 0
	buf[3] = blockTypeKeyIndex
	bytesutil.CopyUint32ToBytes(keyIndex, buf, 4)
	return writer.write(buf)
}

func (writer *SSTableWriter) WriteDataIndex(key []byte, deleted byte, dataIndex uint32) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte delete flag
	1 - byte block type
	4 - bytes dataIndex
	4 - bytes key length
	bytes for key
	*/
	buf := make([]byte, 12+len(key))
	buf[0] = keyMagicCode1
	buf[1] = keyMagicCode2
	buf[2] = deleted
	buf[3] = blockTypeKey
	bytesutil.CopyUint32ToBytes(dataIndex, buf, 4)
	bytesutil.CopyUint32ToBytes(uint32(len(key)), buf, 8)
	bytesutil.CopyDataToBytes(key, 0, buf, 12, len(key))
	return writer.write(buf)
}

func (writer *SSTableWriter) WriteData(key []byte, data *Data) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte delete flag
	1 - byte block type
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
	headerAndKey[3] = blockTypeData
	dataSum := hashutil.SumHash32(data.value)
	bytesutil.CopyUint32ToBytes(dataSum, headerAndKey, 4)
	bytesutil.CopyUint64ToBytes(data.ts, headerAndKey, 8)
	bytesutil.CopyUint32ToBytes(uint32(len(key)), headerAndKey, 16)
	bytesutil.CopyUint32ToBytes(uint32(len(data.value)), headerAndKey, 20)
	bytesutil.CopyDataToBytes(key, 0, headerAndKey, 24, len(key))
	dataIndex, err := writer.write(headerAndKey)
	if err != nil {
		return 0, nil
	}
	_, err = writer.write(data.value)
	if err != nil {
		return 0, nil
	}
	return dataIndex, nil
}

func (writer *SSTableWriter) Close() error {
	return nil
}


func (writer *SSTableWriter) Commit() error {
	return nil
}
