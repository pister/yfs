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
	"github.com/pister/yfs/common/listutil"
	"sort"
	lg "github.com/pister/yfs/log"
	"github.com/pister/yfs/common/maputil/switching"
	"github.com/pister/yfs/common/fileutil"
	"github.com/pister/yfs/common/lockutil"
	"github.com/pister/yfs/common/atomicutil"
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
	dataSize *atomicutil.AtomicInt64
}

func OpenAheadLog(filename string) (*AheadLog, error) {
	wal := new(AheadLog)
	if err := wal.openFile(filename); err != nil {
		return nil, err
	}
	return wal, nil
}

func (wal *AheadLog) openFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	wal.file = file
	wal.closed = false
	wal.filename = filename
	fi, err := file.Stat()
	if err != nil {
		return nil
	}
	wal.dataSize = atomicutil.NewAtomicInt64(fi.Size())
	return nil
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
	return wal.dataSize.Get()
}

func (wal *AheadLog) Append(action *Action) error {
	wLen, err := action.WriteTo(wal.file)
	if err != nil {
		return err
	}
	wal.dataSize.Add(int64(wLen))
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

func (wal *AheadLog) DeleteFile() error {
	if err := wal.file.Close(); err != nil {
		return err
	}
	if err := fileutil.DeleteFile(wal.filename); err != nil {
		return err
	}
	return nil
}

const (
	maxMemData = 10 * 1024 * 1024
)

type BlockData struct {
	deleted deletedFlag
	ts      uint64
	value   []byte
}

var log lg.Logger

func init() {
	l, err := lg.NewLogger("lsm")
	if err != nil {
		panic(err)
	}
	log = l
}

type Lsm struct {
	aheadLog    *AheadLog
	memMap      *switching.SwitchingMap
	sstReaders  *listutil.CopyOnWriteList // type of *SSTableReader
	mutex       sync.Mutex
	flushLocker *lockutil.TryLocker
	dir         string
	ts          int64
}

type TsFileName struct {
	pathName string
	ts       int64
}

type walWrapper struct {
	aheadLog *AheadLog
	memMap   *maputil.SafeTreeMap
	ts       int64
}

func loadExistWal(dir string) (*walWrapper, error) {
	walNamePattern, err := regexp.Compile(`^wal_\d+$`)
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
	ww := new(walWrapper)
	walFile := walFiles[0]
	wal, err := OpenAheadLog(walFile.pathName)
	if err != nil {
		return nil, err
	}
	ww.aheadLog = wal
	ww.memMap = maputil.NewSafeTreeMap()
	if err := wal.initLsmMemMap(ww.memMap); err != nil {
		return nil, err
	}
	ww.ts = walFile.ts
	return ww, nil
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

func loadSSTableReaders(dir string) (*listutil.CopyOnWriteList, error) {
	sstNamePattern, err := regexp.Compile(`^sst_[abc]_\d+$`)
	if err != nil {
		return nil, err
	}
	tsFiles := make([]TsFileName, 0, 32)
	if err := filepath.Walk(dir, func(file string, info os.FileInfo, err error) error {
		_, name := path.Split(file)
		if sstNamePattern.MatchString(name) {

			post := strings.LastIndex(name, "_")
			ts, err := strconv.ParseInt(name[post+1:], 10, 64)
			if err != nil {
				return err
			}
			tsFiles = append(tsFiles, TsFileName{file, ts})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Sort(sstFileSlice(tsFiles))
	sstables := make([]interface{}, 0, len(tsFiles))
	for _, tsFile := range tsFiles {
		log.Info("reading sst file: %s", tsFile.pathName)

		sst, err := OpenSSTableReader(tsFile.pathName)
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, sst)
	}
	return listutil.NewCopyOnWriteListWithInitData(sstables), nil
}

func newWalWrapper(dir string) (*walWrapper, error) {
	ww := new(walWrapper)
	ts := time.Now().Unix()
	walFileName := fmt.Sprintf("%s%c%s_%d", dir, filepath.Separator, "wal", ts)
	wal, err := OpenAheadLog(walFileName)
	if err != nil {
		return nil, err
	}
	ww.memMap = maputil.NewSafeTreeMap()
	ww.aheadLog = wal
	ww.ts = ts
	return ww, nil
}

// 检查wal文件，如果有，则加载，否则新建
func OpenLsm(dir string) (*Lsm, error) {
	ww, err := loadExistWal(dir)
	if err != nil {
		return nil, err
	}
	if ww == nil {
		ww, err = newWalWrapper(dir)
		if err != nil {
			return nil, err
		}
	}
	lsm := new(Lsm)
	lsm.aheadLog = ww.aheadLog
	lsm.memMap = switching.NewSwitchingMapWithInitData(ww.memMap)
	lsm.dir = dir
	lsm.ts = ww.ts
	lsm.flushLocker = lockutil.NewTryLocker()
	sstReaders, err := loadSSTableReaders(dir)
	if err != nil {
		return nil, err
	}
	lsm.sstReaders = sstReaders

	return lsm, nil
}

func (lsm *Lsm) NeedFlush() bool {
	return lsm.aheadLog.GetDataSize() > maxMemData
}

func (lsm *Lsm) Close() error {
	lsm.flushLocker.Lock()
	defer lsm.flushLocker.Unlock()
	lsm.aheadLog.Close()
	lsm.memMap = nil
	lsm.sstReaders = nil
	return nil
}

func (lsm *Lsm) Put(key []byte, value []byte) error {
	if value == nil {
		return fmt.Errorf("value can not be nil")
	}
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
	if lsm.NeedFlush() {
		err := lsm.Flush()
		if err != nil {
			log.Info("start flush error", err)
		}
	}
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

func (lsm *Lsm) findBlockDataFromSSTables(key []byte) (*BlockData, error) {
	var blockDataFound *BlockData = nil
	if err := lsm.sstReaders.Foreach(func(item interface{}) (bool, error) {
		sstReader := item.(*SSTableReader)
		blockData, err := sstReader.GetByKey(key)
		if err != nil {
			return true, err
		}
		if blockData != nil {
			blockDataFound = blockData
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	return blockDataFound, nil
}

func (lsm *Lsm) Get(key []byte) ([]byte, error) {
	ds, found := lsm.memMap.Get(key)
	if found {
		bd := ds.(*BlockData)
		if bd.deleted == deleted {
			return nil, nil
		}
		return bd.value, nil
	}
	bd, err := lsm.findBlockDataFromSSTables(key)
	if err != nil {
		return nil, err
	}
	if bd != nil {
		if bd.deleted == deleted {
			return nil, nil
		} else {
			return bd.value, nil
		}
	} else {
		return nil, nil
	}
}

type dataIndex struct {
	key       []byte
	dataIndex uint32
}

func flushImpl(dir string, ww *walWrapper) (*SSTableReader, error) {
	sst, err := NewSSTableWriter(dir, sstLevelA, ww.ts)
	if err != nil {
		return nil, err
	}
	bloomFilter := bloom.NewUnsafeBloomFilter(bloomBitSizeFromLevel(sstLevelA))
	if err := sst.WriteMemMap(ww.memMap, func(key []byte) {
		bloomFilter.Add(key)
	}); err != nil {
		return nil, err
	}
	if err := sst.Close(); err != nil {
		return nil, err
	}
	// delete WAL log
	if err := ww.aheadLog.DeleteFile(); err != nil {
		return nil, err
	}
	// rename
	if err := sst.Commit(); err != nil {
		return nil, err
	}
	// open sst reader
	sstReader, err := OpenSSTableReaderWithBloomFilter(sst.fileName, bloomFilter)
	if err != nil {
		return nil, err
	}
	return sstReader, nil
}

func (lsm *Lsm) Flush() error {
	lsm.flushLocker.Lock()

	ww, err := newWalWrapper(lsm.dir)
	if err != nil {
		lsm.flushLocker.Unlock()
		return err
	}

	log.Info("start flushing...")

	oldWW := new(walWrapper)
	oldWW.aheadLog = lsm.aheadLog
	oldWW.memMap = lsm.memMap.SwitchNew()
	oldWW.ts = lsm.ts

	lsm.aheadLog = ww.aheadLog
	lsm.ts = ww.ts

	go func() {
		defer lsm.flushLocker.Unlock()
		reader, err := flushImpl(lsm.dir, oldWW)
		if err != nil {
			lsm.memMap.MergeToMain()
			log.Info("flush fail:", err)
		} else {
			lsm.sstReaders.Add(reader)
			lsm.memMap.CleanSwitch()
			log.Info("flush finish.")
		}

	}()
	return nil
}
