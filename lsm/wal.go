package lsm

import (
	"os"
	"github.com/pister/yfs/common/atomicutil"
	"github.com/pister/yfs/common/maputil"
	"github.com/pister/yfs/common/fileutil"
	"io"
	"github.com/pister/yfs/lsm/base"
)

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

func (wal *AheadLog) initToMemMap(treeMap *maputil.SafeTreeMap) error {
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
			ds := new(base.BlockData)
			ds.Value = action.value
			ds.Ts = action.ts
			switch action.op {
			case actionTypePut:
				ds.Deleted = base.Normal
			case actionTypeDelete:
				ds.Deleted = base.Deleted
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
