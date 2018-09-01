package write

import (
	"os"
	"github.com/pister/yfs/common/maputil"
	"time"
	"sync"
)

type AheadLog struct {
	file *os.File
}

func (wal *AheadLog) Append(action *Action) error {
	return action.WriteTo(wal.file)
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

func (lsm *Lsm) FlushAndClose() error {
	lsm.flushMutex.Lock()
	defer lsm.flushMutex.Unlock()
	// 1, open to a temp file from memTable
	sstable, err := NewSSTableWriter("", "")
	if err != nil {
		return err
	}
	/*
	data1,
	data2,
	...
	dataN
	key-dataIndex1,  <- key-dataIndex-start-pos
	key-dataIndex2,
	...
	key-dataIndexN
	key-dataIndex-start-pos
	*/

	// 2, write

	lsm.memMap.Foreach(func(key []byte, value interface{}) {
		dataIndex, err := sstable.WriteData(key, value.(*Data))
	})


	// 3, add index, footer etc...

	// 4, delete WAL log

	// 5, release and close resources.
}
