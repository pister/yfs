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
	"github.com/pister/yfs/common/bloom"
	"sync/atomic"
	"github.com/pister/yfs/common/listutil"
)

type deletedFlag byte

const (
	normal  deletedFlag = 0
	deleted             = 1
)

const (
	maxKeyLen   = 2 * 1024
	maxValueLen = 20 * 1024 * 1024
)

var sstNamePattern *regexp.Regexp

func init() {
	p, err := regexp.Compile(`sst_[abc]_\d+`)
	if err != nil {
		panic(err)
	}
	sstNamePattern = p
}

type AheadLog struct {
	file     *os.File
	closed   bool
	filename string
	dataSize int64
}

func OpenAheadLog(filename string) (*AheadLog, error) {
	log := new(AheadLog)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	log.file = file
	log.closed = false
	log.filename = filename
	fi, err := file.Stat()
	if err != nil {
		return nil, nil
	}
	log.dataSize = fi.Size()
	return log, nil
}

func (wal *AheadLog) initLsmMemMap(treeMap *maputil.SafeTreeMap) error {
	_, err := treeMap.SafeOperate(func(unsafeMap *maputil.TreeMap) (interface{}, error) {
		for {
			action, err := ActionFromReader(wal.file)
			if err != nil {
				if err != io.EOF {
					return nil, err
				} else {
					break
				}
			}
			ds := new(BlockData)
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

func (wal *AheadLog) GetDataSize() int64 {
	return atomic.LoadInt64(&wal.dataSize)
}

func (wal *AheadLog) Append(action *Action) error {
	wLen, err := action.WriteTo(wal.file)
	if err != nil {
		return err
	}
	atomic.AddInt64(&wal.dataSize, int64(wLen))
	return nil
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

func (wal *AheadLog) DeleteTemp() error {
	if err := wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(wal.filename + "_tmp"); err != nil {
		return err
	}
	return nil
}

const (
	maxMemData = 20 * 1024 * 1024
)

type BlockData struct {
	deleted deletedFlag
	ts      uint64
	value   []byte
}

type Lsm struct {
	aheadLog   *AheadLog
	memMap     *maputil.SafeTreeMap
	sstReader  *listutil.CopyOnWriteList // type of *SSTableReader
	mutex      sync.Mutex
	flushMutex sync.Mutex
	dir        string
	ts         int64
}

type TsFileName struct {
	pathName string
	ts       int64
}

func tryOpenExistWal(dir string) (*Lsm, error) {
	walNamePattern, err := regexp.Compile(`wal_\d+`)
	if err != nil {
		return nil, err
	}
	walFiles := make([]TsFileName, 0, 3)
	filepath.Walk(dir, func(file string, info os.FileInfo, err error) error {
		_, name := path.Split(file)
		if walNamePattern.MatchString(name) {
			post := strings.LastIndex(name, "_")
			ts, err := strconv.ParseInt(name[post+1:], 10, 64)
			if err != nil {
				return err
			}
			walFiles = append(walFiles, TsFileName{file, ts})
		}
		return nil
	})
	if len(walFiles) <= 0 {
		return nil, nil
	}
	walFile := walFiles[0]
	wal, err := OpenAheadLog(walFile.pathName)
	if err != nil {
		return nil, err
	}
	lsm := new(Lsm)
	lsm.aheadLog = wal
	lsm.memMap = maputil.NewSafeTreeMap()
	if err := wal.initLsmMemMap(lsm.memMap); err != nil {
		return nil, err
	}
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
	wal, err := OpenAheadLog(walFileName)
	if err != nil {
		return nil, err
	}
	lsm = new(Lsm)
	lsm.aheadLog = wal
	lsm.memMap = maputil.NewSafeTreeMap()
	lsm.dir = dir
	lsm.ts = ts
	return lsm, nil
}

func (lsm *Lsm) NeedFlush() bool {
	return lsm.aheadLog.GetDataSize() > maxMemData
}


func (lsm *Lsm) appendBloomFilter(filter bloom.Filter) {

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
	ds := new(BlockData)
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
	ds := new(BlockData)
	ds.value = nil
	ds.ts = action.ts
	ds.deleted = deleted
	lsm.memMap.Put(key, ds)
	return nil
}

type sstFileSlice []TsFileName

func (sf sstFileSlice) Len() int {
	return len(sf)
}

func (sf sstFileSlice) Less(i, j int) bool {
	return sf[i].ts > sf[j].ts
}

func (sf sstFileSlice) Swap(i, j int) {
	tmp := sf[i]
	sf[i] = sf[j]
	sf[j] = tmp
}

func (lsm *Lsm) findBlockDataFromSSTable(key []byte, sst TsFileName) (*BlockData, bool) {
	// 1. get form cache
	// 2. search from file

	return nil, false
}

func (lsm *Lsm) findBlockDataFromSSTables(key []byte) (*BlockData, bool) {
	/*
	sstFiles := make([]TsFileName, 0, 20)
	filepath.Walk(lsm.dir, func(file string, info os.FileInfo, err error) error {
		_, name := path.Split(file)
		if sstNamePattern.MatchString(name) {
			post := strings.LastIndex(name, "_")
			ts, err := strconv.ParseInt(name[post+1:], 10, 64)
			if err != nil {
				return err
			}
			sstFiles = append(sstFiles, TsFileName{file, ts})
		}
		return nil
	})
	sort.Sort(sstFileSlice(sstFiles))
	for _, sstFile := range sstFiles {
		fmt.Println(sstFile)
	}*/

	return nil, false
}

func (lsm *Lsm) Get(key []byte) ([]byte, bool) {
	ds, found := lsm.memMap.Get(key)
	if found {
		bd := ds.(*BlockData)
		if bd.deleted == deleted {
			return nil, false
		}
		return bd.value, true
	}
	bd, found := lsm.findBlockDataFromSSTables(key)
	if found {
		if bd.deleted == deleted {
			return nil, false
		}
		return bd.value, true
	} else {
		return nil, false
	}

}

type dataIndex struct {
	key       []byte
	dataIndex uint32
}

func (lsm *Lsm) FlushAndClose() (string, error) {
	lsm.flushMutex.Lock()
	defer lsm.flushMutex.Unlock()
	sst, err := NewSSTableWriter(lsm.dir, sstLevelA, lsm.ts)
	if err != nil {
		return "", err
	}

	if err := sst.WriteMemMap(lsm.memMap); err != nil {
		return "", err
	}

	if err := lsm.aheadLog.RenameToTemp(); err != nil {
		return "", err
	}
	if err := sst.Close(); err != nil {
		return "", err
	}
	// 4, delete WAL log
	if err := lsm.aheadLog.DeleteTemp(); err != nil {
		return "", err
	}
	// 5, rename
	if err := sst.Commit(); err != nil {
		return "", err
	}
	// 6, make bloom-filter as key cache
	return sst.fileName, nil
}
