package write

import (
	"os"
	"github.com/pister/yfs/common/maputil"
	"time"
	"sync"
)

type AheadLog struct {
	file     *os.File
	closed   bool
	filename string
}

func (wal *AheadLog) Append(action *Action) error {
	return action.WriteTo(wal.file)
}

func (wal *AheadLog) Close() error {
	if wal.closed {
		return nil
	}
	if err := wal.file.Close(); err != nil {
		return err
	}
	wal.closed = true
	return nil
}

func (wal *AheadLog) RenameToTemp() error {
	if err := wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(wal.filename, wal.filename + "_tmp"); err != nil {
		return err
	}
	return nil
}

func (wal *AheadLog) Delete() error {
	if err := wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(wal.filename); err != nil {
		return err
	}
	return nil
}

const (
	maxMemData = 20 * 1024 * 1024
)

type Data struct {
	deleted byte
	ts      uint64
	value   []byte
}

type Lsm struct {
	aheadLog   *AheadLog
	memMap     *maputil.SafeTreeMap
	dataSize   int64
	flushMutex sync.Mutex
}

func NewLsm() *Lsm {
	// TODO
	return nil
}

func (lsm *Lsm) NeedFlush() bool {
	return lsm.dataSize > maxMemData
}

func (lsm *Lsm) Put(key []byte, value []byte) error {
	// write wal
	action := new(Action)
	action.op = actionTypePut
	action.key = key
	action.value = value
	action.ts = uint64(time.Now().Unix())
	err := lsm.aheadLog.Append(action)
	if err != nil {
		return err
	}
	// update mem
	ds := new(Data)
	ds.value = value
	ds.ts = action.ts
	ds.deleted = 0
	lsm.memMap.Put(key, ds)
	return nil
}

func (lsm *Lsm) Delete(key []byte) error {
	// write wal
	action := new(Action)
	action.op = actionTypeDelete
	action.key = key
	action.ts = uint64(time.Now().Unix())
	err := lsm.aheadLog.Append(action)
	if err != nil {
		return err
	}
	// update mem
	ds := new(Data)
	ds.value = nil
	ds.ts = action.ts
	ds.deleted = 1
	lsm.memMap.Put(key, ds)
	return nil
}

type dataIndex struct {
	key       []byte
	deleted   byte
	dataIndex uint32
}

func (lsm *Lsm) FlushAndClose() error {
	lsm.flushMutex.Lock()
	defer lsm.flushMutex.Unlock()
	// 1, open to a temp file from memTable
	sstable, err := NewSSTableWriter("", "")
	if err != nil {
		return err
	}

	/*
	the sstable data format:
	block-data-0
	block-data-1
	block-data-2
	...
	block-data-N
	data-index-0    <- data-index-start-position
	data-index-1
	data-index-2
	....
	key-data-index-N
	key-index-0			<- key-index-start-position
	key-index-1
	key-index-2
	...
	key-index-N
	footer:data-index-start-position,key-index-start-position
	 */
	dataLength := lsm.memMap.Length()
	// 2, write data
	dataIndexes := make([]*dataIndex, 0, dataLength)
	lsm.memMap.Foreach(func(key []byte, value interface{}) bool {
		data := value.(*Data)
		index, e := sstable.WriteData(key, data)
		if e != nil {
			err = e
			return true
		}
		dataIndexes = append(dataIndexes, &dataIndex{key, data.deleted, index})
		return false
	})
	if err != nil {
		return err
	}
	// 3, write data index
	keyIndexes := make([]uint32, 0, dataLength)
	for _, di := range dataIndexes {
		keyIndex, err := sstable.WriteDataIndex(di.key, di.deleted, di.dataIndex)
		if err != nil {
			return err
		}
		keyIndexes = append(keyIndexes, keyIndex)
	}
	dataIndexStartPosition := keyIndexes[0]
	var keyIndexStartPosition uint32 = 0
	// 4, write key index
	for _, keyIndex := range keyIndexes {
		p, err := sstable.writeKeyIndex(keyIndex)
		if err != nil {
			return err
		}
		if keyIndexStartPosition == 0 {
			keyIndexStartPosition = p
		}
	}
	// writer footer
	if err := sstable.WriteFooter(dataIndexStartPosition, keyIndexStartPosition); err != nil {
		return err
	}

	if err := lsm.aheadLog.RenameToTemp(); err != nil {
		return err
	}
	if err := sstable.Close(); err != nil {
		return err
	}
	// 4, delete WAL log
	if err := lsm.aheadLog.Delete(); err != nil {
		return err
	}
	// 5, rename
	if err := sstable.Commit(); err != nil {
		return err
	}
	return nil
}
