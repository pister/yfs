package lsm

import (
	"os"
	"sync"
	"fmt"
	"path/filepath"
	"strings"
	"strconv"
	"path"
	"github.com/pister/yfs/common/listutil"
	"sort"
	lg "github.com/pister/yfs/log"
	"github.com/pister/yfs/common/maputil/switching"
	"github.com/pister/yfs/common/lockutil"
	"github.com/pister/yfs/common/fileutil"
	"github.com/pister/yfs/lsm/sst"
	"github.com/pister/yfs/lsm/base"
	"github.com/pister/yfs/common/lockutil/process"
)

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
	flushLocker lockutil.TryLocker
	dirLocker   lockutil.TryLocker
	dir         string
	ts          int64
}

func loadSSTableReaders(dir string) (*listutil.CopyOnWriteList, error) {
	tsFiles := make([]base.TsFileName, 0, 32)
	if err := filepath.Walk(dir, func(file string, info os.FileInfo, err error) error {
		_, name := path.Split(file)
		if base.SSTNamePattern.MatchString(name) {
			post := strings.LastIndex(name, "_")
			ts, err := strconv.ParseInt(name[post+1:], 10, 64)
			if err != nil {
				return err
			}
			tsFiles = append(tsFiles, base.TsFileName{file, ts})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Sort(base.SSTFileSlice(tsFiles))
	sstables := make([]interface{}, 0, len(tsFiles))
	for _, tsFile := range tsFiles {
		log.Info("reading sst file: %s", tsFile.PathName)

		sst, err := sst.OpenSSTableReader(tsFile.PathName)
		if err != nil {
			return nil, err
		}
		if sst == nil {
			log.Info("ignore empty sst file: %s", tsFile.PathName)
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
			fileutil.DeleteFile(tsFile.PathName)
			log.Info("wal %s size is 0. just delete it", tsFile.PathName)
			continue
		}
		_, _, err = WalFileToSSTable(dir, ww)
		if err != nil {
			return err
		}
		log.Info("processed wal to sst: %s", tsFile.PathName)
	}
	return nil
}

func OpenLsm(dir string) (*Lsm, error) {
	fileutil.MkDirs(dir)
	dirLocker := process.OpenLocker(fmt.Sprintf("%s/lsm_lock", dir))
	if !dirLocker.TryLock() {
		return nil, fmt.Errorf("the lsm dir: %s has opend by another proccess", dir)
	}
	if err := prepareForOpenLsm(dir); err != nil {
		dirLocker.Unlock()
		return nil, err
	}
	ww, err := createOrOpenFirstWalWrapper(dir)
	if err != nil {
		dirLocker.Unlock()
		return nil, err
	}
	lsm := new(Lsm)
	lsm.dirLocker = dirLocker
	lsm.aheadLog = ww.aheadLog
	lsm.memMap = switching.NewSwitchingMapWithMainData(ww.memMap)
	lsm.dir = dir
	lsm.ts = ww.ts
	lsm.flushLocker = lockutil.NewTryLocker()
	sstReaders, err := loadSSTableReaders(dir)
	if err != nil {
		dirLocker.Unlock()
		return nil, err
	}
	lsm.sstReaders = sstReaders
	return lsm, nil
}

func (lsm *Lsm) NeedFlush() bool {
	return lsm.aheadLog.GetDataSize() > base.MaxMemData
}

func (lsm *Lsm) Close() error {
	lsm.flushLocker.Lock()
	defer lsm.flushLocker.Unlock()

	lsm.aheadLog.Close()
	// release the dir locker
	lsm.dirLocker.Unlock()

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
	action.ts = uint64(base.GetCurrentTs())
	err := lsm.aheadLog.Append(action)
	if err != nil {
		return err
	}
	// update mem
	ds := new(base.BlockData)
	ds.Value = value
	ds.Ts = action.ts
	ds.Deleted = base.Normal
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
	action.ts = uint64(base.GetCurrentTs())
	err := lsm.aheadLog.Append(action)
	if err != nil {
		return err
	}
	// update mem
	ds := new(base.BlockData)
	ds.Value = nil
	ds.Ts = action.ts
	ds.Deleted = base.Deleted
	lsm.memMap.Put(key, ds)
	return nil
}

func (lsm *Lsm) findBlockDataFromSSTables(key []byte) (*base.BlockData, error) {
	var blockDataFound *base.BlockData = nil
	if err := lsm.sstReaders.Foreach(func(item interface{}) (bool, error) {
		sstReader := item.(*sst.SSTableReader)
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
		bd := ds.(*base.BlockData)
		if bd.Deleted == base.Deleted {
			return nil, nil
		}
		return bd.Value, nil
	}
	bd, err := lsm.findBlockDataFromSSTables(key)
	if err != nil {
		return nil, err
	}
	if bd != nil {
		if bd.Deleted == base.Deleted {
			return nil, nil
		} else {
			return bd.Value, nil
		}
	} else {
		return nil, nil
	}
}

func (lsm *Lsm) Flush() error {
	if !lsm.flushLocker.TryLock() {
		return nil
	}

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
			reader, err := sst.OpenSSTableReaderWithBloomFilter(sstFilePath, filter)
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

func (lsm *Lsm) Compact() error {

	return nil
}
