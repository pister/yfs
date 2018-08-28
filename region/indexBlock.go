package region

import (
	"os"
	"sync"
	"github.com/pister/yfs/naming"
	"github.com/pister/yfs/utils"
	"fmt"
)

type IndexBlock struct {
	file     os.File
	mutex    sync.RWMutex
	blockId  uint32
	position uint32
	regionId uint16

	readCache map[dataIndex]dataIndex // map[index]block
}

const (
	indexMagicCode            = 'I'
	indexItemLength           = 12
	indexMaxBlockSize         = 24 * 1024 * 1024
	indexFlagNormal           = 0
	indexFlagDeleted          = 1
	indexConcurrentWriteBlock = 16
)

func (indexBlock *IndexBlock) write(data []byte) error {
	_, err := indexBlock.file.Write(data)
	if err != nil {
		return err
	}
	indexBlock.position += uint32(len(data))
	return nil
}

func (indexBlock *IndexBlock) reset(pos uint32) error {
	_, err := indexBlock.file.Seek(int64(pos), 0)
	if err != nil {
		return err
	}
	indexBlock.position = pos
	return nil
}

func (indexBlock *IndexBlock) Add(di dataIndex) (*naming.Name, error) {
	indexBlock.mutex.Lock()
	defer indexBlock.mutex.Unlock()

	indexPosition := indexBlock.position
	if indexItemLength+indexPosition > indexMaxBlockSize {
		return nil, fmt.Errorf("not enough size for this region[%d]", indexBlock.blockId)
	}

	// =================== FORMAT START ==================
	// 1 bytes magic code
	// 1 bytes delete flag
	// 4 bytes data-region-id
	// 4 bytes data-dataPosition
	// 2 bytes sum16 of (data-region-id and data-dataPosition)
	// =================== FORMAT END ====================

	buf := make([]byte, indexItemLength, indexItemLength)
	// magic code
	buf[0] = indexMagicCode
	// delete flag
	buf[1] = indexFlagNormal
	// data region id
	utils.CopyUint32ToBytes(uint32(di.blockId), buf, 2)
	// data dataPosition
	utils.CopyUint32ToBytes(uint32(di.position), buf, 6)
	// sum16 of (data-region-id and data-dataPosition)
	sumVal := utils.SumHash16(buf[2:10])
	utils.CopyUint16ToBytes(sumVal, buf, 10)

	err := indexBlock.write(buf)
	if err != nil {
		indexBlock.reset(indexPosition)
		// how about when resetDataFile err ?
		// here is just ignore
		return nil, err
	}
	name := new(naming.Name)
	name.IndexBlockId = indexBlock.blockId
	name.IndexPosition = indexPosition
	name.RegionId = indexBlock.regionId

	// for read readCache
	cacheKey := dataIndex{blockId: name.IndexBlockId, position: name.IndexPosition}
	cacheValue := di
	indexBlock.readCache[cacheKey] = cacheValue

	return name, nil
}

func (indexBlock *IndexBlock) Get(name *naming.Name) (dataIndex, error) {
	indexBlock.mutex.RLock()
	defer indexBlock.mutex.RUnlock()
	cacheKey := dataIndex{blockId: name.IndexBlockId, position: name.IndexPosition}
	value, exist := indexBlock.readCache[cacheKey]
	if exist {
		return value, nil
	}
	return dataIndex{}, fmt.Errorf("not exist")
}

func (indexBlock *IndexBlock) Delete(name *naming.Name) error {
	if indexBlock.regionId != name.RegionId {
		return fmt.Errorf("regionId not match")
	}
	if indexBlock.blockId != name.IndexBlockId {
		return fmt.Errorf("indexBlockId not match")
	}

	indexBlock.mutex.Lock()
	defer indexBlock.mutex.Unlock()

	if name.IndexPosition >= indexMaxBlockSize-indexItemLength {
		return fmt.Errorf("invalidate dataPosition")
	}
	_, err := indexBlock.file.Seek(int64(name.IndexPosition), 0)
	if err != nil {
		return err
	}
	buf := make([]byte, indexItemLength, indexItemLength)
	n, err := indexBlock.file.Read(buf)
	if err != nil {
		return err
	}
	if n < indexItemLength {
		return  fmt.Errorf("need read %d bytes", indexItemLength)
	}
	if buf[0] != indexMagicCode {
		return fmt.Errorf("invalidate dataPosition by check magic code fail")
	}

	dataPosition := utils.GetUint32FromBytes(buf, 6)
	if dataPosition > dataMaxBlockSize-dataMetaDataLength {
		return fmt.Errorf("invalidate dataPosition")
	}
	sumFromData := utils.GetUint16FromBytes(buf, 10)
	sumByCal := utils.SumHash16(buf[2:10])
	if sumFromData != sumByCal {
		return fmt.Errorf("check sum fail")
	}

	if buf[1] == indexFlagDeleted {
		return nil
	}
	// DO delete
	buf[1] = indexFlagDeleted
	_, err = indexBlock.file.Seek(int64(name.IndexPosition), 0)
	if err != nil {
		return err
	}
	n, err = indexBlock.file.Write(buf)
	if err != nil {
		return err
	}
	if n < indexItemLength {
		return fmt.Errorf("delete index fail")
	}
	// remove from cache
	cacheKey := dataIndex{blockId: name.IndexBlockId, position: name.IndexPosition}
	delete(indexBlock.readCache, cacheKey)
	return nil
}