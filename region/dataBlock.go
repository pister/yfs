package block

import (
	"os"
	"github.com/pister/yfs/naming"
	"sync"
	"fmt"
	"github.com/pister/yfs/utils"
)

const (
	dataMagicCode_0    = 'D'
	dataMagicCode_1    = 'B'
	dataFlagNormal     = 0
	dataFlagDeleted    = 1
	dataMaxBlockSize   = 128 * 1024 * 1024
	dataHeaderLength   = 8
	dataMetaDataLength = dataHeaderLength + 4
)

type dataIndex struct {
	blockId  uint32
	position uint32
}

type WriteDataBlock struct {
	file     os.File
	blockId  uint32
	mutex    sync.Mutex
	position uint32
}

func NewBlockStore(fileDir string, index int) (*WriteDataBlock, error) {
	// TODO
	return nil, nil
}

func OpenBlockStore(fileDir string, index int) (*WriteDataBlock, error) {
	return nil, nil
}

func (bs *WriteDataBlock) write(data []byte) error {
	_, err := bs.file.Write(data)
	if err != nil {
		return err
	}
	bs.position += uint32(len(data))
	return nil
}

func (bs *WriteDataBlock) reset(pos uint32) error {
	_, err := bs.file.Seek(int64(pos), 0)
	if err != nil {
		return err
	}
	bs.position = pos
}

func (bs *WriteDataBlock) Add(data []byte) (dataIndex, error) {
	if data == nil {
		return dataIndex{}, fmt.Errorf("data is empty")
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	dataLen := len(data)
	fullLen := dataLen + dataMetaDataLength
	dataPosition := bs.position
	if uint32(fullLen)+dataPosition > dataMaxBlockSize {
		return dataIndex{}, fmt.Errorf("not enough size for this block[%d]", bs.blockId)
	}

	dataSum := utils.SumHash32(data)
	// =================== FORMAT START ==================
	// 2 bytes magic code
	// 4 bytes data len
	// 1 bytes delete flag
	// 1 bytes reserved
	// the real data begin ...
	// ----
	// the real data finish ...
	// 4 bytes sumHash
	// =================== FORMAT END ====================

	buf := make([]byte, fullLen, fullLen)
	// magic code
	buf[0] = dataMagicCode_0
	buf[1] = dataMagicCode_1
	// data len
	utils.CopyUint32ToBytes(uint32(dataLen), buf, 2)
	// delete flag
	buf[6] = dataFlagNormal
	// reserved
	buf[7] = 0

	utils.CopyDataToBytes(data, 0, buf, 8, dataLen)
	utils.CopyUint32ToBytes(dataSum, buf, dataLen+8)

	err := bs.write(buf)
	if err != nil {
		bs.reset(dataPosition)
		// how about when reset err ?
		// here is just ignore
		return dataIndex{}, err
	}
	di := dataIndex{}
	di.position = uint32(dataPosition)
	di.blockId = bs.blockId
	return di, nil
}

func (bs *WriteDataBlock) Get(name dataIndex) ([]byte, error) {
	if bs.blockId != name.blockId {
		return nil, fmt.Errorf("blockId is not match")
	}
	if name.position >= dataMaxBlockSize-dataMetaDataLength {
		return nil, fmt.Errorf("invalidate position")
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	if name.position >= bs.position-dataMetaDataLength {
		return nil, fmt.Errorf("invalidate position")
	}
	_, err := bs.file.Seek(int64(name.Index), 0)
	if err != nil {
		return nil, err
	}
	header := make([]byte, dataHeaderLength, dataHeaderLength)
	n, err := bs.file.Read(header)
	if err != nil {
		return nil, err
	}
	if n < dataHeaderLength {
		return nil, fmt.Errorf("need read %d bytes", dataHeaderLength)
	}
	if header[0] != dataMagicCode_0 || header[1] != dataMagicCode_1 {
		return nil, fmt.Errorf("invalidate position by check magic code fail")
	}
	dataLen := utils.GetUint32FromBytes(header, 2)
	if dataLen > dataMaxBlockSize-name.position-4 {
		return nil, fmt.Errorf("invalidate data length")
	}
	deleteFlag := header[6]
	if deleteFlag == dataFlagDeleted {
		return nil, fmt.Errorf("data not exist")
	}
	// include read sum hash
	data := make([]byte, dataLen+4, dataLen+4)
	n, err = bs.file.Read(data)
	if err != nil {
		return nil, err
	}
	if n < 0 || uint32(n) < dataLen+4 {
		return nil, fmt.Errorf("need read %d bytes", dataLen+4)
	}
	sumHashFromData := utils.GetUint32FromBytes(data, dataLen)
	theData := data[:dataLen]
	sumHashByCal := utils.SumHash32(theData)
	if sumHashFromData != sumHashByCal {
		return nil, fmt.Errorf("check sum fail")
	}
	return theData, nil
}
func (bs *WriteDataBlock) Exist(name *naming.Name) (bool, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	// TODO
	return false, nil
}

func (bs *WriteDataBlock) Delete(name *naming.Name) error {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	// TODO

	return nil

}
