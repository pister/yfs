package sst

import (
	"os"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
	"fmt"
	"path/filepath"
	"github.com/pister/yfs/common/bloom"
	"github.com/pister/yfs/lsm/base"
)

type SSTableWriter struct {
	file         *os.File
	position     uint32
	fileName     string
	tempFileName string
}

func (writer *SSTableWriter) GetFileName() string {
	return writer.fileName
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

func (writer *SSTableWriter) WriteFooter(dataIndexStartPosition uint32, bloomFilterPosition uint32) error {
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
	buf[3] = BlockTypeFooter
	bytesutil.CopyUint32ToBytes(dataIndexStartPosition, buf, 4)
	bytesutil.CopyUint32ToBytes(bloomFilterPosition, buf, 8)
	_, err := writer.write(buf)
	return err
}

func (writer *SSTableWriter) WriteDataIndex(key []byte, dataIndex uint32) (uint32, error) {
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
	buf[3] = BlockTypeDataIndex
	bytesutil.CopyUint32ToBytes(dataIndex, buf, 4)
	return writer.write(buf)
}

func (writer *SSTableWriter) WriteBloomFilterData(data []byte, bitLength uint32) (uint32, error) {
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
	buf[3] = BlockTypeBloomFilter
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

func (writer *SSTableWriter) WriteDataBlock(key []byte, data *base.BlockData) (uint32, error) {
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
	headerAndKey[2] = byte(data.Deleted)
	headerAndKey[3] = BlockTypeData
	dataSum := hashutil.SumHash32(data.Value)
	bytesutil.CopyUint32ToBytes(dataSum, headerAndKey, 4)
	bytesutil.CopyUint64ToBytes(data.Ts, headerAndKey, 8)
	bytesutil.CopyUint32ToBytes(uint32(len(key)), headerAndKey, 16)
	bytesutil.CopyUint32ToBytes(uint32(len(data.Value)), headerAndKey, 20)
	bytesutil.CopyDataToBytes(key, 0, headerAndKey, 24, len(key))
	dataIndex, err := writer.write(headerAndKey)
	if err != nil {
		return 0, nil
	}
	_, err = writer.write(data.Value)
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

type ForeachAble interface {
	Foreach(callback func(key []byte, value /*base.BlockData*/ interface{}) bool) error
}

func (writer *SSTableWriter) WriteFullData(memMap ForeachAble) (bloom.Filter, error) {
	bloomFilter := bloom.NewUnsafeBloomFilter(bloomBitSizeFromLevel(LevelA))
	var err error
	// 1, write data
	dataIndexes := make([]*base.DataIndex, 0, 64)
	memMap.Foreach(func(key []byte, value interface{}) bool {
		data := value.(*base.BlockData)
		index, e := writer.WriteDataBlock(key, data)
		if e != nil {
			err = e
			return true
		}
		bloomFilter.Add(key)
		dataIndexes = append(dataIndexes, &base.DataIndex{Key: key, DataIndex: index})
		return false
	})
	if err != nil {
		return nil, err
	}

	// 2, write data index
	keyIndexes := make([]uint32, 0, 64)
	for _, di := range dataIndexes {
		keyIndex, err := writer.WriteDataIndex(di.Key, di.DataIndex)
		if err != nil {
			return nil, err
		}
		keyIndexes = append(keyIndexes, keyIndex)
	}
	dataIndexStartPosition := keyIndexes[0]

	// 3 write bloom filter
	bloomData, bitLength := bloomFilter.GetBitData()
	bloomFilterPosition, err := writer.WriteBloomFilterData(bloomData, bitLength)
	if err != nil {
		return nil, err
	}

	// 4,  writer footer
	if err := writer.WriteFooter(dataIndexStartPosition, bloomFilterPosition); err != nil {
		return nil, err
	}
	return bloomFilter, nil
}
