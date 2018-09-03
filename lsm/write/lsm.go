package write

import (
	"os"
	"github.com/pister/yfs/common/maputil"
	"time"
	"sync"
	"fmt"
	"path/filepath"
	"strings"
	"strconv"
	"path"
	"regexp"
	"io"
)

type deletedFlag byte

const (
	normal  deletedFlag = 0
	deleted             = 1
)

const (
	maxKeyLen = 2 * 1024
	maxValueLen = 20 * 1024 * 1024
)

type AheadLog struct {
	file     *os.File
	closed   bool
	filename string
}

func OpenAheadLog(filename string) (*AheadLog, int64, error) {
	log := new(AheadLog)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, 0, err
	}
	log.file = file
	log.closed = false
	log.filename = filename
	fi, err := file.Stat()
	if err != nil {
		return nil, 0, nil
	}
	return log, fi.Size(), nil
}

func (wal *AheadLog) initLsmMemMap(treeMap *maputil.SafeTreeMap) error {
	_, err := treeMap.SafeOperate(func(unsafeMap *maputil.TreeMap) (interface{}, error ){
		for {
			action, err := ActionFromReader(wal.file)
			if err != nil {
				if err != io.EOF {
					return nil, err
				} else {
					break
				}
			}
			ds := new(blockData)
			ds.value = action.value
			ds.ts = action.ts
			switch action.op {
			case actionTypePut:
				ds.deleted = normal
			case actionTypeDelete:
				ds.deleted = deleted
			}
			unsafeMap.Put(action.key, ds)
		}
		return nil, nil
	})
	return err
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
	if err := os.Rename(wal.filename, wal.filename+"_tmp"); err != nil {
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

type blockData struct {
	deleted deletedFlag
	ts      uint64
	value   []byte
}

type Lsm struct {
	aheadLog *AheadLog
	memMap   *maputil.SafeTreeMap
	dataSize int64
	mutex    sync.Mutex
	dir      string
	ts       int64
}

type walFile struct {
	pathName string
	ts       int64
}

func tryOpenExistWal(dir string) (*Lsm, error) {
	walNamePattern, err := regexp.Compile(`wal_\d+`)
	if err != nil {
		return nil, err
	}
	walFiles := make([]walFile, 0, 3)
	filepath.Walk(dir, func(file string, info os.FileInfo, err error) error {
		_, name := path.Split(file)
		if walNamePattern.MatchString(name) {
			post := strings.LastIndex(name, "_")
			ts, err := strconv.ParseInt(name[post+1:], 10, 64)
			if err != nil {
				return err
			}
			walFiles = append(walFiles, walFile{file, ts})
		}
		return nil
	})
	if len(walFiles) <= 0 {
		return nil, nil
	}
	walFile := walFiles[0]
	wal, fLen, err := OpenAheadLog(walFile.pathName)
	if err != nil {
		return nil, err
	}
	lsm := new(Lsm)
	lsm.aheadLog = wal
	lsm.memMap = maputil.NewSafeTreeMap()
	if err := wal.initLsmMemMap(lsm.memMap); err != nil {
		return nil, err
	}
	lsm.dataSize = fLen
	lsm.dir = dir
	lsm.ts = walFile.ts
	return lsm, nil
}



// 检查wal文件，如果有，则加载，否则新建
func NewLsm(dir string) (*Lsm, error) {
	lsm, err := tryOpenExistWal(dir)
	if err != nil {
		return nil, err
	}
	if lsm != nil {
		return lsm, nil
	}
	ts := time.Now().Unix()
	walFileName := fmt.Sprintf("%s%c%s_%d", dir, filepath.Separator, "wal", ts)
	wal, fLen, err := OpenAheadLog(walFileName)
	if err != nil {
		return nil, err
	}
	lsm = new(Lsm)
	lsm.aheadLog = wal
	lsm.memMap = maputil.NewSafeTreeMap()
	lsm.dataSize = fLen
	lsm.dir = dir
	lsm.ts = ts
	return lsm, nil
}

func (lsm *Lsm) NeedFlush() bool {
	return lsm.dataSize > maxMemData
}

func (lsm *Lsm) Put(key []byte, value []byte) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()
	// write wal
	action := new(Action)
	action.version = defaultVersion
	action.op = actionTypePut
	action.key = key
	action.value = value
	action.ts = uint64(time.Now().Unix())
	err := lsm.aheadLog.Append(action)
	if err != nil {
		return err
	}
	// update mem
	ds := new(blockData)
	ds.value = value
	ds.ts = action.ts
	ds.deleted = normal
	lsm.memMap.Put(key, ds)
	return nil
}

func (lsm *Lsm) Delete(key []byte) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()
	// write wal
	action := new(Action)
	action.version = defaultVersion
	action.op = actionTypeDelete
	action.key = key
	action.ts = uint64(time.Now().Unix())
	err := lsm.aheadLog.Append(action)
	if err != nil {
		return err
	}
	// update mem
	ds := new(blockData)
	ds.value = nil
	ds.ts = action.ts
	ds.deleted = deleted
	lsm.memMap.Put(key, ds)
	return nil
}

func (lsm *Lsm) Get(key []byte) ([]byte, bool) {
	ds, found := lsm.memMap.Get(key)
	if !found {
		return nil, false
	}
	bd := ds.(*blockData)
	if bd.deleted == deleted {
		return nil, false
	}
	return bd.value, true
}

type dataIndex struct {
	key       []byte
	deleted   deletedFlag
	dataIndex uint32
}

func (lsm *Lsm) FlushAndClose() error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()
	// 1, open to a temp file from memTable
	sstable, err := NewSSTableWriter(lsm.dir, lsm.ts)
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
		data := value.(*blockData)
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
