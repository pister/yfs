package lsm

import (
	"os"
	"time"
	"sync"
	"fmt"
	"path/filepath"
	"strings"
	"strconv"
	"path"
	"regexp"
	"github.com/pister/yfs/common/listutil"
	"sort"
	lg "github.com/pister/yfs/log"
	"github.com/pister/yfs/common/maputil/switching"
	"github.com/pister/yfs/common/lockutil"
	"github.com/pister/yfs/common/fileutil"
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
		if sst == nil {
			log.Info("ignore empty sst file: %s", tsFile.pathName)
			continue
		}
		sstables = append(sstables, sst)
	}
	return listutil.NewCopyOnWriteListWithInitData(sstables), nil
}

func prepareForOpenLsm(dir string) error {
	tsFiles, err := getWalFileNames(dir)
	if err != nil {
		return err
	}
	if len(tsFiles) <= 1 {
		return nil
	}
	for _, tsFile := range tsFiles[1:] {
		ww, err := openWalWrapperByTsFile(tsFile)
		if err != nil {
			return err
		}
		if ww.aheadLog.dataSize.Get() == 0 {
			fileutil.DeleteFile(tsFile.pathName)
			log.Info("wal %s size is 0. just delete it", tsFile.pathName)
			continue
		}
		_, _, err = WalFileToSSTable(dir, ww)
		if err != nil {
			return err
		}
		log.Info("processed wal to sst: %s", tsFile.pathName)
	}
	return nil
}

func OpenLsm(dir string) (*Lsm, error) {
	if err := prepareForOpenLsm(dir); err != nil {
		return nil, err
	}
	ww, err := createOrOpenFirstWalWrapper(dir)
	if err != nil {
		return nil, err
	}
	lsm := new(Lsm)
	lsm.aheadLog = ww.aheadLog
	lsm.memMap = switching.NewSwitchingMapWithMainData(ww.memMap)
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
		sstFilePath, filter, err := WalFileToSSTable(lsm.dir, oldWW)
		if err != nil {
			lsm.memMap.MergeToMain()
			log.Info("flush fail:", err)
		} else {
			reader, err := OpenSSTableReaderWithBloomFilter(sstFilePath, filter)
			if err != nil {
				lsm.memMap.MergeToMain()
				log.Info("open sst %s error:", sstFilePath)
			} else {
				lsm.sstReaders.Add(reader)
				lsm.memMap.CleanSwitch()
				log.Info("flush finish.")
			}
		}
	}()
	return nil
}
