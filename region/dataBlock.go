package region

import (
	"os"
	"sync"
	"fmt"
	"github.com/pister/yfs/utils"
)

const (
	dataMagicCode0     = 'D'
	dataMagicCode1     = 'B'
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

type positionIndex struct {
	position    uint32
	deletedFlag byte
}

type DataBlock struct {
	blockId       uint32
	dataFile      os.File
	indexFile     os.File
	mutex         sync.Mutex
	dataPosition  uint32
	indexPosition uint32
	positionCache map[uint32]uint32 // dataPosition-> dataPosition fileChan's indexPosition
}

func NewBlockStore(fileDir string, index int) (*DataBlock, error) {
	// TODO
	return nil, nil
}

func OpenBlockStore(fileDir string, index int) (*DataBlock, error) {
	return nil, nil
}

func (bs *DataBlock) writeDataFile(data []byte) error {
	_, err := bs.dataFile.Write(data)
	if err != nil {
		return err
	}
	bs.dataPosition += uint32(len(data))
	return nil
}

func (bs *DataBlock) writeIndexFile(data []byte) error {
	_, err := bs.indexFile.Write(data)
	if err != nil {
		return err
	}
	bs.indexPosition += uint32(len(data))
	return nil
}

func (bs *DataBlock) resetDataFile(pos uint32) error {
	_, err := bs.dataFile.Seek(int64(pos), 0)
	if err != nil {
		return err
	}
	bs.dataPosition = pos
	return nil
}

func (bs *DataBlock) deleteAtPositionFile(position uint32) {

}

func (bs *DataBlock) Add(data []byte) (dataIndex, error) {
	if data == nil {
		return dataIndex{}, fmt.Errorf("data is empty")
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	dataLen := len(data)
	fullLen := dataLen + dataMetaDataLength
	dataPosition := bs.dataPosition
	if uint32(fullLen)+dataPosition > dataMaxBlockSize {
		return dataIndex{}, fmt.Errorf("not enough size for this region[%d]", bs.blockId)
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

	err := bs.writeDataFile(buf)
	if err != nil {
		bs.resetDataFile(dataPosition)
		// how about when resetDataFile err ?
		// here is just ignore
		return dataIndex{}, err
	}

	di := dataIndex{blockId: bs.blockId, position: uint32(dataPosition)}
	// write finish

	piBuf := make([]byte, 8, 8)
	utils.CopyUint32ToBytes(di.position, piBuf, 0)
	piBuf[4] = dataFlagNormal

	indexPosition := bs.indexPosition
	err = bs.writeIndexFile(piBuf)
	if err != nil {
		bs.resetDataFile(dataPosition)
		// how about when resetDataFile err ?
		// here is just ignore
		return dataIndex{}, err
	}
	bs.positionCache[di.position] = indexPosition
	return di, nil
}

func (bs *DataBlock) Delete(di dataIndex) error {
	if bs.blockId != di.blockId {
		return fmt.Errorf("blockId is not match")
	}
	if di.position >= dataMaxBlockSize-dataMetaDataLength {
		return fmt.Errorf("invalidate dataPosition")
	}
	indexPosition, exist := bs.positionCache[di.position]
	if !exist {
		return nil // not exist!
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	_, err := bs.dataFile.Seek(int64(di.position), 0)
	if err != nil {
		return err
	}
	header := make([]byte, dataHeaderLength, dataHeaderLength)
	n, err := bs.dataFile.Read(header)
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
		return nil
	}
	// Do Delete!
	_, err = bs.dataFile.Seek(int64(di.position), 0)
	if err != nil {
		return err
	}
	// writeDataFile back
	header[6] = dataFlagDeleted
	n, err = bs.dataFile.Write(header)
	if err != nil {
		return err
	}
	if n < dataHeaderLength {
		return fmt.Errorf("delete fail")
	}

	_, err = bs.indexFile.Seek(int64(indexPosition+4), 0)
	if err != nil {
		// FIXME 这里失败了没有回滚
		return err
	}
	// TODO
	//bs.indexFile.Write()
	delete(bs.positionCache, di.position)

	return nil

}

func (bs *DataBlock) Get(di dataIndex) ([]byte, error) {
	if bs.blockId != di.blockId {
		return nil, fmt.Errorf("blockId is not match")
	}
	if di.position >= dataMaxBlockSize-dataMetaDataLength {
		return nil, fmt.Errorf("invalidate dataPosition")
	}
	_, exist := bs.positionCache[di.position]
	if !exist  {
		return nil, fmt.Errorf("not exist")
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	_, err := bs.dataFile.Seek(int64(di.position), 0)
	if err != nil {
		return nil, err
	}
	header := make([]byte, dataHeaderLength, dataHeaderLength)
	n, err := bs.dataFile.Read(header)
	if err != nil {
		return nil, err
	}
	if n < dataHeaderLength {
		return nil, fmt.Errorf("need read %d bytes", dataHeaderLength)
	}
	if header[0] != dataMagicCode0 || header[1] != dataMagicCode1 {
		return nil, fmt.Errorf("invalidate dataPosition by check magic code fail")
	}
	dataLen := utils.GetUint32FromBytes(header, 2)
	if dataLen > dataMaxBlockSize-di.position-4 {
		return nil, fmt.Errorf("invalidate data length")
	}
	deleteFlag := header[6]
	if deleteFlag == dataFlagDeleted {
		return nil, fmt.Errorf("data not exist")
	}
	// include read sum hash
	data := make([]byte, dataLen+4, dataLen+4)
	n, err = bs.dataFile.Read(data)
	if err != nil {
		return nil, err
	}
	if n < 0 || uint32(n) < dataLen+4 {
		return nil, fmt.Errorf("need read %d bytes", dataLen+4)
	}
	sumHashFromData := utils.GetUint32FromBytes(data, int(dataLen))
	theData := data[:dataLen]
	sumHashByCal := utils.SumHash32(theData)
	if sumHashFromData != sumHashByCal {
		return nil, fmt.Errorf("check sum fail")
	}
	return theData, nil
}

func (bs *DataBlock) Exist(di dataIndex) (bool, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	// TODO
	return false, nil
}
