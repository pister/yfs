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
	"time"
	"github.com/pister/yfs/lsm/merge"
)

var log lg.Logger

func init() {
	l, err := lg.NewLogger("lsm")
	if err != nil {
		panic(err)
	}
	log = l
}

const compactThreshold = 3

type Lsm struct {
	aheadLog      *AheadLog
	memMap        *switching.SwitchingMap
	sstReaders    *listutil.CopyOnWriteList // type of *SSTableReader
	mutex         sync.Mutex
	flushLocker   lockutil.TryLocker
	compactLocker lockutil.TryLocker
	dirLocker     lockutil.TryLocker
	dir           string
	ts            int64
	compactTicker *time.Ticker
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
	lsm.compactLocker = lockutil.NewTryLocker()
	sstReaders, err := loadSSTableReaders(dir)
	if err != nil {
		dirLocker.Unlock()
		return nil, err
	}
	lsm.sstReaders = sstReaders
	lsm.compactTicker = time.NewTicker(5 * time.Second)
	lsm.startCompactTask()
	return lsm, nil
}

func (lsm *Lsm) startCompactTask() {
	go func() {
		for range lsm.compactTicker.C {
			if !lsm.needCompact() {
				continue
			}
			err := lsm.Compact()
			if err != nil {
				log.Info("compact error %s", err)
			}
		}
	}()
}

func (lsm *Lsm) NeedFlush() bool {
	return lsm.aheadLog.GetDataSize() > base.MaxMemData
}

func (lsm *Lsm) Close() error {
	lsm.flushLocker.Lock()
	defer lsm.flushLocker.Unlock()

	lsm.compactTicker.Stop()

	lsm.aheadLog.Close()

	// release the dir locker
	lsm.dirLocker.Unlock()

	lsm.sstReaders.Foreach(func(item interface{}) (bool, error) {
		reader := item.(*sst.SSTableReader)
		reader.Close()
		return false, nil
	})
	lsm.sstReaders = nil
	lsm.memMap = nil

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

func (lsm *Lsm) findBlockDataFromSSTables(key []byte, readerTrackers *[]base.ReaderTracker) (*base.BlockData, bool, error) {
	var blockDataFound *base.BlockData = nil
	var resultOpenSuccess bool
	if err := lsm.sstReaders.Foreach(func(item interface{}) (bool, error) {
		sstReader := item.(*sst.SSTableReader)
		blockData, openSuccess, tracker, err := sstReader.GetByKeyWithTrack(key)
		resultOpenSuccess = openSuccess
		if err != nil {
			return true, err
		}
		if readerTrackers != nil {
			*readerTrackers = append(*readerTrackers, tracker)
		}
		if blockData != nil {
			blockDataFound = blockData
			return true, nil
		}
		if !resultOpenSuccess {
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, resultOpenSuccess, err
	}
	return blockDataFound, resultOpenSuccess, nil
}

func (lsm *Lsm) GetWithTracker(key []byte) ([]byte, *base.GetTrackInfo, error) {
	trackInfo := new(base.GetTrackInfo)
	ts := time.Now().UnixNano()
	defer func() {
		trackInfo.EscapeInMillisecond = (time.Now().UnixNano() - ts) / 1000000
	}()
	ds, found := lsm.memMap.Get(key)
	if found {
		trackInfo.InMem = true
		bd := ds.(*base.BlockData)
		if bd.Deleted == base.Deleted {
			return nil, trackInfo, nil
		}
		return bd.Value, trackInfo, nil
	}
	var bd *base.BlockData
	var err error
	var openSuccess bool
	// try 3 times, when compact
	for i := 0; i < 3; i++ {
		trackInfo.ReaderTrackers = make([]base.ReaderTracker, 0, 4)
		bd, openSuccess, err = lsm.findBlockDataFromSSTables(key, &trackInfo.ReaderTrackers)
		if err == nil {
			break
		}
		if !openSuccess {
			// try again
			continue
		}
	}
	if err != nil {
		return nil, trackInfo, err
	}
	if bd != nil {
		if bd.Deleted == base.Deleted {
			return nil, trackInfo, nil
		} else {
			return bd.Value, trackInfo, nil
		}
	} else {
		return nil, trackInfo, nil
	}
}

func (lsm *Lsm) Get(key []byte) ([]byte, error) {
	data, _, err := lsm.GetWithTracker(key)
	return data, err
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
				lsm.sstReaders.AddFirst(reader)
				lsm.memMap.CleanSwitch()
				log.Info("flush finish.")
			}
		}
	}()

	return nil
}

func (lsm *Lsm) getReaders() []*sst.SSTableReader {
	readers := make([]*sst.SSTableReader, 0, 8)
	lsm.sstReaders.Foreach(func(item interface{}) (bool, error) {
		reader := item.(*sst.SSTableReader)
		readers = append(readers, reader)
		return false, nil
	})
	return readers
}

func (lsm *Lsm) needCompact() bool {
	readers := lsm.getReaders()
	return len(readers) >= compactThreshold
}

func selectReadersToCompact(readers []*sst.SSTableReader) []*sst.SSTableReader {
	readersLength := len(readers)
	if readersLength < compactThreshold {
		return nil
	}
	sort.Slice(readers, func(i, j int) bool {
		return readers[i].GetLevel() < readers[j].GetLevel()
	})
	return readers[0:2*readersLength/3]
}

func (lsm *Lsm) Compact() error {
	if !lsm.compactLocker.TryLock() {
		log.Info("need compact, but another compact is not finish.")
		return nil
	}
	defer lsm.compactLocker.Unlock()

	readers := lsm.getReaders()

	if len(readers) < compactThreshold {
		return nil
	}

	log.Info("start compact...")
	readers = selectReadersToCompact(readers)

	compactingFiles := make([]string, 0, len(readers))
	compactingReaders := make([]interface{}, 0, len(readers))
	for _, reader := range readers {
		compactingFiles = append(compactingFiles, reader.GetFileName())
		compactingReaders = append(compactingReaders, reader)
	}

	filter, sstFile, err := merge.CompactFiles(compactingFiles, false)
	if err != nil {
		return err
	}
	reader, err := sst.OpenSSTableReaderWithBloomFilter(sstFile, filter)
	if err != nil {
		// open error
		return err
	}

	lsm.sstReaders.AddLast(reader)
	lsm.sstReaders.Delete(compactingReaders...)

	for _, reader := range compactingReaders {
		r := reader.(*sst.SSTableReader)
		r.Close()
		fileutil.DeleteFile(r.GetFileName())
	}

	log.Info("compact finish.")

	return nil
}
