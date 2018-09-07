package merge

import (
	"os"
	"github.com/pister/yfs/lsm/base"
	"github.com/pister/yfs/lsm/sst"
)

type RichBlockData struct {
	deleted base.DeletedFlag
	ts      uint64
	key     []byte
	value   []byte
}

type sstFileDataBlockReader struct {
	file        *os.File
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
	reader.hasNext = true
	return reader, nil
}

func (sstReader *sstFileDataBlockReader) Close() error {
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

func initSSTReaders(sstFiles []string) ([]*sstFileDataBlockReader, error ){
	readers := make([]*sstFileDataBlockReader, len(sstFiles))
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


func MergeFiles(sstFiles []string, outFile string) error {
	/*
	tmpOutSSTable := outFile + "_tmp"
	readers, err := initSSTReaders(sstFiles)
	if err != nil {
		return err
	}
	defer func(){
		for _, r := range readers {
			r.Close()
		}
	}()


*/
	return nil
}

