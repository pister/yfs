package region

import (
	"sync"
	"fmt"
	"github.com/pister/yfs/utils"
	"io"
)

const (
	dataMagicCode0      = 'D'
	dataMagicCode1      = 'B'
	dataIndexMagicCode1 = 'J'
	dataIndexMagicCode2 = 'K'
	dataFlagNormal      = 0
	dataFlagDeleted     = 1
	dataMaxBlockSize    = 128 * 1024 * 1024
	dataHeaderLength    = 8
	dataMetaDataLength  = dataHeaderLength + 4
	dataBlockFileName   = "block_data"
	dataIndexFileName   = "block_index"
)

type DataIndex struct {
	blockId  uint32
	position uint32
}

// 删除逻辑，只要data和index中其中有一个标记为deleted，就表示已经删除
type BlockStore struct {
	blockId       uint32
	dataFile      *ReadWriteFile
	indexFile     *ReadWriteFile
	mutex         sync.Mutex
	// mapped cache may be very big, we will use bloom-filter instead in future
	positionCache map[uint32]uint32 // dataPosition-> dataPosition fileChan's indexPosition
}

func loadIndexCache(indexFile *ReadWriteFile) (map[uint32]uint32, error) {
	// load cache
	err := indexFile.SeekForReading(0, func(reader io.Reader) error {
		buf := make([]byte, 8)
		for {
			n, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					if n != 8 {
						return fmt.Errorf("bad index data")
					} else {
						// process
					}
				} else {
					return err
				}
			} else {
				// process
			}
		}
		return nil
	})
	// TODO
	return nil, err
}

func OpenBlockStore(dataDir string, blockId uint32, concurrentSize int) (*BlockStore, error) {
	dataBlock := new(BlockStore)
	dataBlock.blockId = blockId
	dataPath := fmt.Sprintf("%s/%s_%d", dataDir, dataBlockFileName, blockId)
	indexPath := fmt.Sprintf("%s/%s_%d", dataDir, dataIndexFileName, blockId)
	dataFile, err := OpenReadWriteFile(dataPath, concurrentSize)
	if err != nil {
		return nil, err
	}
	indexFile, err := OpenReadWriteFile(indexPath, 1)
	if err != nil {
		dataFile.Close()
		return nil, err
	}
	dataBlock.dataFile = dataFile
	dataBlock.indexFile = indexFile
	dataBlock.positionCache = make(map[uint32]uint32)
	// TODO loadIndexCache
	return dataBlock, nil
}

func (bs *BlockStore) writeData(data []byte) (DataIndex, error) {
	dataLen := len(data)
	fullLen := dataLen + dataMetaDataLength
	if int64(fullLen)+bs.dataFile.GetFileLength() > dataMaxBlockSize {
		return DataIndex{}, fmt.Errorf("not enough size for this region[%d]", bs.blockId)
	}
	dataSum := utils.SumHash32(data)
	// =================== DATA FORMAT START ==================
	// TOTAL 12 + len(data) bytes:
	// 2 bytes magic code
	// 4 bytes data len
	// 1 bytes delete flag
	// 1 bytes reserved
	// the real data begin ...
	// ----
	// the real data finish ...
	// 4 bytes sumHash
	// =================== DATA FORMAT END  ====================
	buf := make([]byte, fullLen)
	// magic code
	buf[0] = dataMagicCode0
	buf[1] = dataMagicCode1
	// data len
	utils.CopyUint32ToBytes(uint32(dataLen), buf, 2)
	// delete flag
	buf[6] = dataFlagNormal
	// reserved
	buf[7] = 0

	utils.CopyDataToBytes(data, 0, buf, 8, dataLen)
	utils.CopyUint32ToBytes(dataSum, buf, dataLen+8)

	dataPosition, err := bs.dataFile.Append(buf)
	if err != nil {
		return DataIndex{}, err
	}
	return DataIndex{blockId: bs.blockId, position: uint32(dataPosition)}, nil
}

func (bs *BlockStore) writeIndex(di DataIndex) error {
	// =================== INDEX FORMAT START ==================
	// TOTAL 8 bytes:
	// 2 bytes magic code
	// 1 byte delete flag
	// 1 bytes reserved
	// 4 bytes data position
	// =================== INDEX FORMAT END   ==================
	buf := make([]byte, 8)
	buf[0] = dataIndexMagicCode1
	buf[1] = dataIndexMagicCode2
	buf[2] = dataFlagNormal
	buf[3] = 0 // reserved
	utils.CopyUint32ToBytes(di.position, buf, 4)

	indexPosition, err := bs.indexFile.Append(buf)
	if err != nil {
		// rollback data ?
		return err
	}
	bs.positionCache[di.position] = uint32(indexPosition)
	return nil
}

func (bs *BlockStore) rollbackData(di DataIndex) error {
	_, err := bs.dataFile.Seek(int64(di.position))
	if err != nil {
		return err
	}
	return nil
}

func (bs *BlockStore) deleteData(di DataIndex) error {
	// find index
	indexPosition, exist := bs.positionCache[di.position]
	if exist {
		// delete at index
		err := bs.indexFile.UpdateByteAt(int64(indexPosition+2), dataFlagDeleted)
		if err != nil {
			return err
		}
	}
	// delete data
	return bs.dataFile.UpdateByteAt(int64(di.position+6), dataFlagDeleted)
}

func (bs *BlockStore) Add(data []byte) (DataIndex, error) {
	if data == nil {
		return DataIndex{}, fmt.Errorf("data is empty")
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	di, err := bs.writeData(data)
	if err != nil {
		return di, err
	}
	err = bs.writeIndex(di)
	if err != nil {
		// rollback data
		bs.rollbackData(di)
		// ignore result
		return DataIndex{}, nil
	}
	return di, nil
}

func (bs *BlockStore) Delete(di DataIndex) error {
	if bs.blockId != di.blockId {
		return fmt.Errorf("blockId is not match")
	}
	if di.position >= dataMaxBlockSize-dataMetaDataLength {
		return fmt.Errorf("invalidate dataPosition")
	}
	_, exist := bs.positionCache[di.position]
	if !exist {
		return nil // not exist!
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	err := bs.deleteData(di)
	if err != nil {
		return err
	}
	delete(bs.positionCache, di.position)
	return nil
}

func (bs *BlockStore) Get(di DataIndex) ([]byte, error) {
	if bs.blockId != di.blockId {
		return nil, fmt.Errorf("blockId is not match")
	}
	if di.position >= dataMaxBlockSize-dataMetaDataLength {
		return nil, fmt.Errorf("invalidate dataPosition")
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	_, exist := bs.positionCache[di.position]
	if !exist {
		return nil, fmt.Errorf("not exist")
	}

	header := make([]byte, dataHeaderLength)
	var theData []byte
	bs.dataFile.SeekForReading(int64(di.position), func(reader io.Reader) error {
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
		dataLen := utils.GetUint32FromBytes(header, 2)
		if dataLen > dataMaxBlockSize-di.position-4 {
			return fmt.Errorf("invalidate data length")
		}
		deleteFlag := header[6]
		if deleteFlag == dataFlagDeleted {
			return fmt.Errorf("data not exist")
		}
		// include read sum hash
		data := make([]byte, dataLen+4)
		n, err = reader.Read(data)
		if err != nil {
			return err
		}
		if n < 0 || uint32(n) < dataLen+4 {
			return fmt.Errorf("need read %d bytes", dataLen+4)
		}
		sumHashFromData := utils.GetUint32FromBytes(data, int(dataLen))
		theData = data[:dataLen]
		sumHashByCal := utils.SumHash32(theData)
		if sumHashFromData != sumHashByCal {
			return fmt.Errorf("check sum fail")
		}
		return nil
	})
	return theData, nil
}

func (bs *BlockStore) Exist(di DataIndex) (bool, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	// TODO
	return false, nil
}
