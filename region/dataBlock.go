package region

import (
	"sync"
	"fmt"
	"io"
	"github.com/pister/yfs/common/bloom"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
	"github.com/pister/yfs/common/fileutil"
)

const (
	dataMagicCode0      = 'D'
	dataMagicCode1      = 'B'
	dataIndexMagicCode1 = 'J'
	dataIndexMagicCode2 = 'K'
	dataFlagNormal      = 0
	dataFlagDeleted     = 1
	dataMaxBlockSize    = 128 * 1024 * 1024
	dataHeaderLength    = 12
	dataBlockFileName   = "block_data"
	dataIndexFileName   = "block_index"
)

type DataIndex struct {
	blockId  uint32
	position uint32
}

type dataHeader struct {
	buf []byte
}

func (header *dataHeader) isValidate() bool {
	if len(header.buf) != dataHeaderLength {
		return false
	}
	if header.buf[0] != dataMagicCode0 || header.buf[1] != dataMagicCode1 {
		return false
	}
	return true
}

func (header *dataHeader) isDeleted() bool {
	return header.buf[6] == dataFlagDeleted
}

// 删除逻辑，只要data和index中其中有一个标记为deleted，就表示已经删除
type DataBlock struct {
	blockId   uint32
	dataFile  *fileutil.PositionWriteFile
	indexFile *fileutil.PositionWriteFile
	mutex     sync.Mutex
	// mapped cache may be very big, we will use bloom-filter instead in future
	// positionCache map[uint32]uint32 // dataPosition-> dataPosition fileChan's indexPosition
	positionFilter bloom.Filter
}

func processForLoadIndex(buf []byte, filter bloom.Filter, dataFile *fileutil.PositionWriteFile) error {
	if buf[0] != dataIndexMagicCode1 || buf[1] != dataIndexMagicCode2 {
		return fmt.Errorf("magic not match")
	}
	sumFromData := buf[3]
	sumFromCal := hashutil.SumHash8(buf[4:8])
	if sumFromData != sumFromCal {
		return fmt.Errorf("hash sum not match")
	}
	if buf[2] == dataFlagDeleted {
		// deleted
		return nil
	}
	position := bytesutil.GetUint32FromBytes(buf, 4)

	var normal = false
	// if not delete, to find at block
	dataFile.SeekForReading(int64(position), func(reader io.Reader) error {
		dataBuf := make([]byte, 12)
		_, err := reader.Read(dataBuf)
		if err != nil {
			return err
		}
		dataHeader := dataHeader{buf: dataBuf}
		if !dataHeader.isValidate() {
			return nil
		}
		if !dataHeader.isDeleted() {
			normal = true
		}
		return nil
	})
	if normal {
		filter.Add(buf[4:8])
	}
	return nil
}

func reloadIndexCache(dataFile *fileutil.ReadWriteFile, indexFile *fileutil.ReadWriteFile) (bloom.Filter, error) {
	filter := bloom.NewSafeBloomFilter(1024 * 100)
	// load cache
	err := indexFile.SeekForReading(0, func(reader io.Reader) error {
		buf := make([]byte, 8)
		for {
			_, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					return nil
				} else {
					return err
				}
			} else {
				// process
				err := processForLoadIndex(buf, filter, dataFile)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return filter, err
}



func OpenDataBlock(dataDir string, blockId uint32, concurrentSize int) (*DataBlock, error) {
	dataBlock := new(DataBlock)
	dataBlock.blockId = blockId
	dataPath := fmt.Sprintf("%s/%s_%d", dataDir, dataBlockFileName, blockId)
	indexPath := fmt.Sprintf("%s/%s_%d", dataDir, dataIndexFileName, blockId)
	dataFile, err := fileutil.OpenReadWriteFile(dataPath, concurrentSize)
	if err != nil {
		return nil, err
	}
	indexFile, err := fileutil.OpenReadWriteFile(indexPath, 1)
	if err != nil {
		dataFile.Close()
		return nil, err
	}
	dataBlock.dataFile = dataFile
	dataBlock.indexFile = indexFile
	filter, err := reloadIndexCache(dataFile, indexFile)
	if err != nil {
		return nil, err
	}
	dataBlock.positionFilter = filter
	return dataBlock, nil
}

func (dataBlock *DataBlock) Close() error {
	defer dataBlock.dataFile.Close()
	defer dataBlock.indexFile.Close()
	return nil
}

func (dataBlock *DataBlock) testPositionMayExist(position uint32) bool {
	data := bytesutil.Uint32ToBytes(position)
	return dataBlock.positionFilter.Hit(data)
}

func (dataBlock *DataBlock) AvailableRate() float32 {
	return (dataMaxBlockSize - float32(dataBlock.dataFile.GetFileLength())) / dataMaxBlockSize
}

func (dataBlock *DataBlock) isDataDeleted(position uint32) (bool, error) {
	buf := make([]byte, 12)
	_, err := dataBlock.dataFile.SeekAndReadData(int64(position), buf)
	if err != nil {
		return false, err
	}
	if buf[0] != dataMagicCode0 || buf[1] != dataMagicCode1 {
		return false, fmt.Errorf("magic code not match")
	}
	return buf[6] == dataFlagDeleted, nil
}

func (dataBlock *DataBlock) writeData(data []byte) (DataIndex, error) {
	dataLen := len(data)
	fullLen := dataLen + dataHeaderLength
	if int64(fullLen)+dataBlock.dataFile.GetFileLength() > dataMaxBlockSize {
		return DataIndex{}, fmt.Errorf("not enough size for this data block[%d]", dataBlock.blockId)
	}
	dataSum := hashutil.SumHash32(data)
	// =================== DATA FORMAT START ==================
	// TOTAL 12 + len(data) bytes:
	// 2 bytes magic code
	// 4 bytes data len
	// 1 bytes delete flag
	// 1 bytes reserved
	// 4 bytes sumHash
	// the real data begin ...
	// ----
	// the real data finish ...
	// =================== DATA FORMAT END  ====================
	buf := make([]byte, fullLen)
	// magic code
	buf[0] = dataMagicCode0
	buf[1] = dataMagicCode1
	// data len
	bytesutil.CopyUint32ToBytes(uint32(dataLen), buf, 2)
	// delete flag
	buf[6] = dataFlagNormal
	// reserved
	buf[7] = 0
	bytesutil.CopyUint32ToBytes(dataSum, buf, 8)
	bytesutil.CopyDataToBytes(data, 0, buf, 12, dataLen)
	dataPosition, err := dataBlock.dataFile.Append(buf)
	if err != nil {
		return DataIndex{}, err
	}
	return DataIndex{blockId: dataBlock.blockId, position: uint32(dataPosition)}, nil
}

func (dataBlock *DataBlock) writeIndex(di DataIndex) error {
	// =================== INDEX FORMAT START ==================
	// TOTAL 8 bytes:
	// 2 bytes magic code
	// 1 byte delete flag
	// 1 bytes hash-sum
	// 4 bytes data position
	// =================== INDEX FORMAT END   ==================
	buf := make([]byte, 8)
	buf[0] = dataIndexMagicCode1
	buf[1] = dataIndexMagicCode2
	buf[2] = dataFlagNormal
	bytesutil.CopyUint32ToBytes(di.position, buf, 4)
	buf[3] = hashutil.SumHash8(buf[4:8]) // hash-sum

	_, err := dataBlock.indexFile.Append(buf)
	if err != nil {
		// rollback data ?
		return err
	}
	dataBlock.positionFilter.Add(bytesutil.Uint32ToBytes(di.position))
	return nil
}

func (dataBlock *DataBlock) rollbackData(di DataIndex) error {
	_, err := dataBlock.dataFile.Seek(int64(di.position))
	if err != nil {
		return err
	}
	return nil
}

func (dataBlock *DataBlock) deleteData(di DataIndex) error {
	if !dataBlock.testPositionMayExist(di.position) {
		// not exist
		return nil
	}
	// here not update delete flag in index file, because of update index file is very slow
	// delete data
	return dataBlock.dataFile.UpdateByteAt(int64(di.position+6), dataFlagDeleted)
}

func (dataBlock *DataBlock) Add(data []byte) (DataIndex, error) {
	if data == nil {
		return DataIndex{}, fmt.Errorf("data is empty")
	}
	dataBlock.mutex.Lock()
	defer dataBlock.mutex.Unlock()

	di, err := dataBlock.writeData(data)
	if err != nil {
		return di, err
	}
	err = dataBlock.writeIndex(di)
	if err != nil {
		// rollback data
		dataBlock.rollbackData(di)
		// ignore result
		return DataIndex{}, nil
	}
	return di, nil
}

func (dataBlock *DataBlock) Delete(di DataIndex) error {
	if dataBlock.blockId != di.blockId {
		return fmt.Errorf("blockId is not match")
	}
	if di.position >= dataMaxBlockSize-dataHeaderLength {
		return fmt.Errorf("invalidate dataPosition")
	}

	dataBlock.mutex.Lock()
	defer dataBlock.mutex.Unlock()

	err := dataBlock.deleteData(di)
	if err != nil {
		return err
	}
	return nil
}

func (dataBlock *DataBlock) Get(di DataIndex) ([]byte, error) {
	if !dataBlock.testPositionMayExist(di.position) {
		// not exist
		return nil, fmt.Errorf("not exist")
	}
	if dataBlock.blockId != di.blockId {
		return nil, fmt.Errorf("blockId is not match")
	}
	if di.position >= dataMaxBlockSize-dataHeaderLength {
		return nil, fmt.Errorf("invalidate dataPosition")
	}
	dataBlock.mutex.Lock()
	defer dataBlock.mutex.Unlock()

	header := make([]byte, dataHeaderLength)
	var theData []byte
	dataBlock.dataFile.SeekForReading(int64(di.position), func(reader io.Reader) error {
		n, err := reader.Read(header)
		if err != nil {
			return err
		}
		if n < dataHeaderLength {
			return fmt.Errorf("need read %d bytes", dataHeaderLength)
		}
		if header[0] != dataMagicCode0 || header[1] != dataMagicCode1 {
			return fmt.Errorf("invalidate dataPosition by check magic code fail")
		}
		dataLen := bytesutil.GetUint32FromBytes(header, 2)
		if dataLen > dataMaxBlockSize-di.position-4 {
			return fmt.Errorf("invalidate data length")
		}
		deleteFlag := header[6]
		if deleteFlag == dataFlagDeleted {
			return fmt.Errorf("data not exist")
		}
		sumHashFromData := bytesutil.GetUint32FromBytes(header, 8)
		// include read sum hash
		data := make([]byte, dataLen)
		n, err = reader.Read(data)
		if err != nil {
			return err
		}
		if n < 0 || uint32(n) < dataLen {
			return fmt.Errorf("need read %d bytes", dataLen+4)
		}
		theData = data[:dataLen]
		sumHashByCal := hashutil.SumHash32(theData)
		if sumHashFromData != sumHashByCal {
			return fmt.Errorf("check sum fail")
		}
		return nil
	})
	return theData, nil
}

func (dataBlock *DataBlock) Exist(di DataIndex) (bool, error) {
	dataBlock.mutex.Lock()
	defer dataBlock.mutex.Unlock()
	// TODO
	return false, nil
}
