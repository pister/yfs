package lsm

import (
	"os"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
	"fmt"
	"path/filepath"
	"github.com/pister/yfs/common/maputil"
	"github.com/pister/yfs/common/bloom"
)

type SSTableWriter struct {
	file         *os.File
	position     uint32
	fileName     string
	tempFileName string
}

func NewSSTableWriter(dir string, level SSTableLevel, ts int64) (*SSTableWriter, error) {
	ssTableWriter := new(SSTableWriter)
	fileName := fmt.Sprintf("%s%c%s_%s_%d", dir, filepath.Separator, "sst", level.Name(), ts)
	tempFileName := fileName + "_tmp"
	file, err := os.OpenFile(tempFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	ssTableWriter.fileName = fileName
	ssTableWriter.tempFileName = tempFileName
	ssTableWriter.file = file
	ssTableWriter.position = 0
	return ssTableWriter, nil
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

func (writer *SSTableWriter) writeFooter(dataIndexStartPosition uint32, bloomFilterPosition uint32) error {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes data-index-start-position-Index
	4 - bytes bloom-filter-position
	*/
	buf := make([]byte, 12)
	buf[0] = footerMagicCode1
	buf[1] = footerMagicCode2
	buf[2] = 0
	buf[3] = blockTypeFooter
	bytesutil.CopyUint32ToBytes(dataIndexStartPosition, buf, 4)
	bytesutil.CopyUint32ToBytes(bloomFilterPosition, buf, 8)
	_, err := writer.write(buf)
	return err
}

func (writer *SSTableWriter) writeDataIndex(key []byte, dataIndex uint32) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes dataIndex
	*/
	buf := make([]byte, 8)
	buf[0] = dataIndexMagicCode1
	buf[1] = dataIndexMagicCode2
	buf[2] = 0
	buf[3] = blockTypeDataIndex
	bytesutil.CopyUint32ToBytes(dataIndex, buf, 4)
	return writer.write(buf)
}

func (writer *SSTableWriter) writeBloomFilterData(data []byte, bitLength uint32) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bit set length
	4 - bloom filter data length
	...bytes for bloom filter
	*/
	buf := make([]byte, 12)
	buf[0] = bloomFilterMagicCode1
	buf[1] = bloomFilterMagicCode2
	buf[2] = 0
	buf[3] = blockTypeBloomFilter
	bytesutil.CopyUint32ToBytes(bitLength, buf, 4)
	bytesutil.CopyUint32ToBytes(uint32(len(data)), buf, 8)
	pos, err := writer.write(buf)
	if err != nil {
		return 0, err
	}
	if _, err := writer.write(data); err != nil {
		return 0, err
	}
	return pos, nil
}

func (writer *SSTableWriter) writeData(key []byte, data *BlockData) (uint32, error) {
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
	headerAndKey[2] = byte(data.deleted)
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
	return writer.file.Close()
}

func (writer *SSTableWriter) Commit() error {
	if err := os.Rename(writer.tempFileName, writer.fileName); err != nil {
		return err
	}
	return nil
}

func (writer *SSTableWriter) WriteMemMap(memMap *maputil.SafeTreeMap) (bloom.Filter, error) {
	bloomFilter := bloom.NewUnsafeBloomFilter(bloomBitSizeFromLevel(sstLevelA))
	dataLength := memMap.Length()
	if dataLength == 0 {
		return nil, nil
	}
	var err error
	// 1, write data
	dataIndexes := make([]*dataIndex, 0, dataLength)
	memMap.Foreach(func(key []byte, value interface{}) bool {
		data := value.(*BlockData)
		index, e := writer.writeData(key, data)
		if e != nil {
			err = e
			return true
		}
		bloomFilter.Add(key)
		dataIndexes = append(dataIndexes, &dataIndex{key, index})
		return false
	})
	if err != nil {
		return nil, err
	}

	// 2, write data index
	keyIndexes := make([]uint32, 0, dataLength)
	for _, di := range dataIndexes {
		keyIndex, err := writer.writeDataIndex(di.key, di.dataIndex)
		if err != nil {
			return nil, err
		}
		keyIndexes = append(keyIndexes, keyIndex)
	}
	dataIndexStartPosition := keyIndexes[0]

	// 3 write bloom filter
	bloomData, bitLength := bloomFilter.GetBitData()
	bloomFilterPosition, err := writer.writeBloomFilterData(bloomData, bitLength)
	if err != nil {
		return nil, err
	}

	// 4,  writer footer
	if err := writer.writeFooter(dataIndexStartPosition, bloomFilterPosition); err != nil {
		return nil, err
	}
	return bloomFilter, nil
}
