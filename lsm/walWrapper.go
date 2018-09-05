package lsm

import (
	"path"
	"strings"
	"strconv"
	"github.com/pister/yfs/common/maputil"
	"path/filepath"
	"os"
	"sort"
	"fmt"
	"time"
	"regexp"
	"github.com/pister/yfs/common/bloom"
)

var walNamePattern *regexp.Regexp

func init() {
	p, err := regexp.Compile(`^wal_\d+$`)
	if err != nil {
		panic(err)
	}
	walNamePattern = p
}

type walWrapper struct {
	aheadLog *AheadLog
	memMap   *maputil.SafeTreeMap
	ts       int64
}

func getWalFileNames(dir string) ([]TsFileName, error) {
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
	sort.Sort(sstFileSlice(walFiles))
	return walFiles, nil
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

func createOrOpenFirstWalWrapper(dir string) (*walWrapper, error) {
	walFiles, err := getWalFileNames(dir)
	if err != nil {
		return nil, err
	}
	if len(walFiles) == 0 {
		wal, err := newWalWrapper(dir)
		if err != nil {
			return nil, err
		}
		return wal, nil
	} else {
		return openWalWrapperByTsFile(walFiles[0])
	}
}

func openWalWrapperByTsFile(walFile TsFileName) (*walWrapper, error) {
	ww := new(walWrapper)
	wal, err := OpenAheadLog(walFile.pathName)
	if err != nil {
		return nil, err
	}
	ww.memMap = maputil.NewSafeTreeMap()
	wal.initToMemMap(ww.memMap)
	ww.aheadLog = wal
	ww.ts = walFile.ts
	return ww, nil
}

func WalFileToSSTable(dir string, ww *walWrapper) (string, bloom.Filter, error) {
	sst, err := NewSSTableWriter(dir, sstLevelA, ww.ts)
	if err != nil {
		return "", nil, err
	}
	bloomFilter, err := sst.WriteMemMap(ww.memMap)
	if err := sst.Close(); err != nil {
		return "", nil, err
	}
	// delete WAL log
	if err := ww.aheadLog.DeleteFile(); err != nil {
		return "", nil, err
	}
	// rename
	if err := sst.Commit(); err != nil {
		return "", nil, err
	}
	return sst.fileName, bloomFilter, nil
}