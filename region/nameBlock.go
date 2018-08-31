package region

import (
	"sync"
	"github.com/pister/yfs/naming"
	"fmt"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
	"io"
	"github.com/pister/yfs/common/maputil"
)

type NameBlock struct {
	blockId   uint32
	regionId  uint16
	mutex     sync.Mutex
	file      *ReadWriteFile
	readCache *maputil.SafeMap // map[index]block
}

const (
	nameMagicCode     = 'N'
	nameItemLength    = 12
	nameMaxBlockSize  = 24 * 1024 * 1024
	nameFlagNormal    = 0
	nameFlagDeleted   = 1
	nameBlockFileName = "name_block"
)

func initReadCache(nameBlockId uint32, file *ReadWriteFile) (*maputil.SafeMap, error) {
	m := maputil.NewSafeMap()
	err := file.SeekForReading(0, func(reader io.Reader) error {
		namePosition := 0
		buf := make([]byte, 12)
		for ; ; namePosition += 12 {
			_, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					return nil
				} else {
					return err
				}
			} else {
				if buf[0] != nameMagicCode {
					return fmt.Errorf("magic code not match")
				}
				if buf[1] == nameFlagDeleted {
					continue
				}
				sumFromData := bytesutil.GetUint16FromBytes(buf, 2)
				sumByCal := hashutil.SumHash16(buf[4:12])
				if sumFromData != sumByCal {
					return fmt.Errorf("sum not match")
				}
				blockId := bytesutil.GetUint32FromBytes(buf, 4)
				positionId := bytesutil.GetUint32FromBytes(buf, 8)
				cacheKey := fmt.Sprintf("%d-%d", nameBlockId, namePosition)
				cacheValue := DataIndex{blockId: blockId, position: positionId}
				m.Put(cacheKey, cacheValue)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, err
}

func OpenNameBlock(regionId uint16, dataDir string, blockId uint32, concurrentSize int) (*NameBlock, error) {
	nameBlock := new(NameBlock)
	nameBlock.regionId = regionId
	nameBlock.blockId = blockId
	namePath := fmt.Sprintf("%s/%s_%d", dataDir, nameBlockFileName, blockId)
	nameFile, err := OpenReadWriteFile(namePath, concurrentSize)
	if err != nil {
		return nil, err
	}
	nameBlock.file = nameFile
	readCache, err := initReadCache(blockId, nameBlock.file)
	if err != nil {
		nameBlock.file.Close()
		return nil, err
	}
	nameBlock.readCache = readCache
	return nameBlock, nil
}

func (indexBlock *NameBlock) Close() error {
	indexBlock.file.Close()
	return nil
}

func (indexBlock *NameBlock) Add(di DataIndex) (*naming.Name, error) {
	indexBlock.mutex.Lock()
	defer indexBlock.mutex.Unlock()

	if nameItemLength+indexBlock.file.GetFileLength() > nameMaxBlockSize {
		return nil, fmt.Errorf("not enough size for this name block[%d]", indexBlock.blockId)
	}

	// =================== FORMAT START ==================
	// total 12 bytes:
	// 1 bytes magic code
	// 1 bytes delete flag
	// 2 bytes sum16 of (data-region-id and data-dataPosition)
	// 4 bytes data-region-id
	// 4 bytes data-dataPosition
	// =================== FORMAT END ====================

	buf := make([]byte, nameItemLength, nameItemLength)
	// magic code
	buf[0] = nameMagicCode
	// delete flag
	buf[1] = nameFlagNormal
	// data region id
	bytesutil.CopyUint32ToBytes(uint32(di.blockId), buf, 4)
	// data dataPosition
	bytesutil.CopyUint32ToBytes(uint32(di.position), buf, 8)
	// sum16 of (data-region-id and data-dataPosition)
	sumVal := hashutil.SumHash16(buf[4:12])
	bytesutil.CopyUint16ToBytes(sumVal, buf, 2)

	namePosition, err := indexBlock.file.Append(buf)
	if err != nil {
		return nil, err
	}
	name := new(naming.Name)
	name.IndexBlockId = indexBlock.blockId
	name.IndexPosition = uint32(namePosition)
	name.RegionId = indexBlock.regionId

	// for read readCache
	cacheKey := fmt.Sprintf("%d-%d", name.IndexBlockId, name.IndexPosition)
	cacheValue := di
	indexBlock.readCache.Put(cacheKey, cacheValue)
	return name, nil
}

func (indexBlock *NameBlock) Get(name *naming.Name) (DataIndex, bool, error) {
	indexBlock.mutex.Lock()
	defer indexBlock.mutex.Unlock()

	cacheKey := fmt.Sprintf("%d-%d", name.IndexBlockId, name.IndexPosition)
	value, exist := indexBlock.readCache.Get(cacheKey)
	if exist {
		return value.(DataIndex), true, nil
	}
	return DataIndex{}, false, nil
}

func (indexBlock *NameBlock) Delete(name *naming.Name) error {
	if indexBlock.regionId != name.RegionId {
		return fmt.Errorf("regionId not match")
	}
	if indexBlock.blockId != name.IndexBlockId {
		return fmt.Errorf("indexBlockId not match")
	}

	indexBlock.mutex.Lock()
	defer indexBlock.mutex.Unlock()

	if name.IndexPosition >= nameMaxBlockSize-nameItemLength {
		return fmt.Errorf("invalidate dataPosition")
	}
	err := indexBlock.file.UpdateByteAt(int64(name.IndexPosition+1), nameFlagDeleted)
	if err != nil {
		return err
	}
	// remove from cache
	cacheKey := fmt.Sprintf("%d-%d", name.IndexBlockId, name.IndexPosition)
	indexBlock.readCache.Delete(cacheKey)
	return nil
}
