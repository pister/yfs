package block

import (
	"os"
	"sync"
	"github.com/pister/yfs/naming"
	"github.com/pister/yfs/utils"
	"fmt"
)

type WriteIndexBlock struct {
	file     os.File
	regionId uint16
	mutex    sync.Mutex
	blockId  uint32
	position uint32
}

const (
	indexMagicCode    = 'I'
	indexItemLength   = 12
	indexMaxBlockSize = 24 * 1024 * 1024
	indexFlagNormal   = 0
	indexFlagDeleted  = 1
)

func (indexBlock *WriteIndexBlock) write(data []byte) error {
	_, err := indexBlock.file.Write(data)
	if err != nil {
		return err
	}
	indexBlock.position += uint32(len(data))
	return nil
}

func (indexBlock *WriteIndexBlock) reset(pos uint32) error {
	_, err := indexBlock.file.Seek(int64(pos), 0)
	if err != nil {
		return err
	}
	indexBlock.position = pos
}

func (indexBlock *WriteIndexBlock) Add(di dataIndex) (*naming.Name, error) {
	indexBlock.mutex.Lock()
	defer indexBlock.mutex.Unlock()

	indexPosition := indexBlock.position
	if indexItemLength+indexPosition > indexMaxBlockSize {
		return nil, fmt.Errorf("not enough size for this block[%d]", bs.blockId)
	}

	// =================== FORMAT START ==================
	// 1 bytes magic code
	// 1 bytes delete flag
	// 4 bytes data-block-id
	// 4 bytes data-position
	// 2 bytes sum16 of (data-block-id and data-position)
	// =================== FORMAT END ====================

	buf := make([]byte, indexItemLength, indexItemLength)
	// magic code
	buf[0] = indexMagicCode
	// delete flag
	buf[1] = indexFlagNormal
	// data block id
	utils.CopyUint32ToBytes(uint32(di.blockId), buf, 2)
	// data position
	utils.CopyUint32ToBytes(uint32(di.position), buf, 6)
	// sum16 of (data-block-id and data-position)
	sumVal := utils.SumHash16(buf[2:10])
	utils.CopyUint16ToBytes(sumVal, buf, 10)

	err := indexBlock.write(buf)
	if err != nil {
		indexBlock.reset(indexPosition)
		// how about when reset err ?
		// here is just ignore
		return nil, err
	}
	name := new(naming.Name)
	name.IndexBlockId = indexBlock.blockId
	name.IndexPosition = indexPosition
	name.RegionId = indexBlock.regionId
	return name, nil

}
