package merge

import (
	"os"
	"github.com/pister/yfs/lsm/base"
	"github.com/pister/yfs/lsm/sst"
	"path/filepath"
	"github.com/pister/yfs/common/bloom"
	"strings"
	"strconv"
	"sort"
	"github.com/pister/yfs/common/fileutil"
)

type RichBlockData struct {
	deleted base.DeletedFlag
	ts      uint64
	key     []byte
	value   []byte
}

type sstFileDataBlockReader struct {
	file        *os.File
	fileName    string
	currentData *RichBlockData
	hasNext     bool
}

func OpenSstFileDataBlockReader(fileName string) (*sstFileDataBlockReader, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	reader := new(sstFileDataBlockReader)
	reader.file = file
	reader.fileName = fileName
	reader.hasNext = true
	return reader, nil
}

func (sstReader *sstFileDataBlockReader) Close() error {
	if sstReader.file == nil {
		return nil
	}
	return sstReader.file.Close()
}

func (sstReader *sstFileDataBlockReader) PeekNextData() (*RichBlockData, error) {
	if sstReader.currentData != nil {
		return sstReader.currentData, nil
	}
	if !sstReader.hasNext {
		return nil, nil
	}
	rbd, err := sstReader.readNext()
	if err != nil {
		return nil, err
	}
	if rbd == nil {
		sstReader.hasNext = false
		return nil, nil
	}
	sstReader.currentData = rbd
	return sstReader.currentData, nil
}

func (sstReader *sstFileDataBlockReader) readNext() (*RichBlockData, error) {
	header, err := sst.ReadDataHeader(sstReader.file)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	dataBuf := make([]byte, header.ValueLength)
	_, err = sstReader.file.Read(dataBuf)
	if err != nil {
		return nil, err
	}
	if header.BlockType != sst.BlockTypeData {
		return nil, nil
	}
	rbd := new(RichBlockData)
	rbd.deleted = header.Deleted
	rbd.ts = header.Ts
	rbd.key = header.Key
	rbd.value = dataBuf
	return rbd, nil
}

func (sstReader *sstFileDataBlockReader) PopNextData() (*RichBlockData, error) {
	if sstReader.currentData != nil {
		ret := sstReader.currentData
		sstReader.currentData = nil
		return ret, nil
	}
	if !sstReader.hasNext {
		return nil, nil
	}
	rbd, err := sstReader.readNext()
	if err != nil {
		return nil, err
	}
	if rbd == nil {
		sstReader.hasNext = false
		return nil, nil
	}
	return rbd, nil
}

func initSSTReaders(sstFiles []string) ([]*sstFileDataBlockReader, error) {
	readers := make([]*sstFileDataBlockReader, 0, len(sstFiles))
	for _, file := range sstFiles {
		reader, err := OpenSstFileDataBlockReader(file)
		if err != nil {
			for _, r := range readers {
				r.Close()
			}
			return nil, err
		}
		readers = append(readers, reader)
	}
	return readers, nil
}

type dataAndReader struct {
	data   *RichBlockData
	reader *sstFileDataBlockReader
}

func getToBeUseData(readers []*sstFileDataBlockReader) (*dataAndReader, error) {
	var retValue *dataAndReader = nil
	for _, reader := range readers {
		data, err := reader.PeekNextData()
		if err != nil {
			return nil, err
		}
		if data == nil {
			continue
		}
		if retValue == nil {
			retValue = &dataAndReader{data, reader}
			continue
		}
		compareResult := sst.KeyCompare(retValue.data.key, data.key)
		switch compareResult {
		case sst.Equals:
			if data.ts > retValue.data.ts {
				// ignore the less one
				retValue.reader.PopNextData()
				retValue = &dataAndReader{data, reader}
			} else {
				// ignore the less one
				reader.PopNextData()
			}
		case sst.Greater:
			retValue = &dataAndReader{data, reader}
		case sst.Less:
			// do nothing
		default:
			// do nothing
		}
	}
	return retValue, nil
}

type fileDataBlockReaders struct {
	readers []*sstFileDataBlockReader
}

func (readers *fileDataBlockReaders) Foreach(callback func(key []byte, value interface{}) bool) error {
	for {
		da, err := getToBeUseData(readers.readers)
		if err != nil {
			return err
		}
		if da == nil {
			return nil
		}
		bd := new(base.BlockData)
		bd.Ts = da.data.ts
		bd.Deleted = da.data.deleted
		bd.Value = da.data.value
		da.reader.PopNextData()
		callback(da.data.key, bd)
	}
	return nil
}

func merge(readers []*sstFileDataBlockReader, dir string, level uint32, ts int64, deleteOldFiles bool) (bloom.Filter, string, error) {
	writer, err := sst.NewSSTableWriter(dir, level, ts)
	if err != nil {
		return nil, "", err
	}
	fdbReaders := &fileDataBlockReaders{readers: readers}
	bloomFilter, err := writer.WriteFullData(level, fdbReaders)
	if err := writer.Close(); err != nil {
		return nil, "", err
	}

	if err := writer.Commit(); err != nil {
		return nil, "", err
	}
	if deleteOldFiles {
		for _, reader := range readers {
			fileutil.DeleteFile(reader.fileName)
		}
	}
	return bloomFilter, writer.GetFileName(), nil
}

func CompactFiles(files []string, deleteOldFiles bool) (bloom.Filter, string, error) {
	if len(files) == 0 {
		return nil, "", nil
	}
	maxLevel := 0
	sstFiles := make([]base.TsFileName, 0, len(files))
	for _, file := range files {
		_, name := filepath.Split(file)
		if base.SSTNamePattern.MatchString(name) {
			parts := strings.Split(name, "_")
			level, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, "", err
			}
			if level > maxLevel {
				maxLevel = level
			}
			ts, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				return nil, "", err
			}
			sstFiles = append(sstFiles, base.TsFileName{PathName: file, Ts: ts})
		}
	}
	sort.Sort(base.SSTFileSlice(sstFiles))
	readers, err := initSSTReaders(files)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		for _, r := range readers {
			if r != nil {
				r.Close()
			}
		}
	}()
	file := sstFiles[len(sstFiles)-1]
	dir, _ := filepath.Split(file.PathName)
	return merge(readers, dir, uint32(maxLevel)+1, file.Ts, deleteOldFiles)
}
