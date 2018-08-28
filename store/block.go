package store

import (
	"os"
	"github.com/pister/yfs/name"
	"sync"
	"fmt"
	"github.com/pister/yfs/utils"
)

var magicCode []byte

func init() {
	magicCode = make([]byte, 0, 2)
	magicCode = append(magicCode, 65, 72)
}

func sumHash(data []byte) uint32 {
	var s uint32 = 0
	for _, d := range data {
		s = s*11 + 13*uint32(d)
	}
	return s
}

type BlockStore struct {
	file    os.File
	blockId uint16
	mutex   sync.RWMutex
	pos     int64
}

func NewBlockStore(fileDir string, index int) (*BlockStore, error) {
	// TODO
	return nil, nil
}

func OpenBlockStore(fileDir string, index int) (*BlockStore, error) {
	return nil, nil
}

func (bs *BlockStore) write(data []byte) error {
	_, err := bs.file.Write(data)
	if err != nil {
		return err
	}
	bs.pos += int64(len(data))
	return nil
}

func (bs *BlockStore) reset(pos int64) error {
	_, err := bs.file.Seek(pos, 0)
	if err != nil {
		return err
	}
	bs.pos = pos

	// TODO
}

func (bs *BlockStore) Add(data []byte) (name.Name, error) {
	if data == nil {
		return name.Name{}, fmt.Errorf("data is empty.")
	}
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	dataLen := uint32(len(data))
	dataPosition := bs.pos

	dataSum := sumHash(data)
	dataSumData := utils.Uint32ToBytes(dataSum)

	// 2 bytes magic code
	// 4 bytes data len
	// 1 bytes delete flag
	// 1 bytes reserved
	// start data
	// ----
	// 4 bytes sumHash
	header := make([]byte, 8, 8)
	// magic code
	utils.CopyDataToBytes(magicCode, 0, header, 0, 2)
	// data len
	utils.CopyUint32ToBytes(dataLen, header, 2)
	// delete flag
	header[6] = 0
	// reserved
	header[7] = 0

	err := bs.write(header)
	if err != nil {
		return name.Name{}, err
	}
	err = bs.write(data)
	if err != nil {
		bs.reset(dataPosition)
		// how about when reset err ?
		// here is just ignore
		return name.Name{}, err
	}
	err = bs.write(dataSumData)
	if err != nil {
		bs.reset(dataPosition)
		bs.reset(dataPosition)
		// how about when reset err ?
		// here is just ignore
		return name.Name{}, err
	}
	// finish
	name := name.Name{}

	name.Reserved = 0
	name.PositionIndex = uint16(dataPosition)
	name.BlockIndex = bs.blockId
	name.NameServerIndex = 0 // TODO

	return name, nil
}

func (bs *BlockStore) Delete(name []byte) error {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	// TODO

	return nil

}
func (bs *BlockStore) Get(name []byte) ([]byte, error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	// TODO
	return nil, nil
}
func (bs *BlockStore) Exist(name []byte) (bool, error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	// TODO
	return false, nil
}
