package region

import (
	"os"
)

type ConcurrentReadFile struct {
	fileChan chan *os.File
	size     int
}

type PositionWriteFile struct {
	fileChan        chan *os.File
	writingPosition uint32
}

func OpenAsConcurrentReadFile(path string, concurrentSize int) (*ConcurrentReadFile, error) {
	cfReader := new(ConcurrentReadFile)
	cfReader.size = concurrentSize
	cfReader.fileChan = make(chan *os.File, concurrentSize)
	tempFiles := make([]*os.File, 0, concurrentSize)
	var err error = nil
	for i := 0; i < concurrentSize; i++ {
		var fp *os.File = nil
		fp, err = os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			break
		}
		tempFiles = append(tempFiles, fp)
		cfReader.fileChan <- fp
	}
	for err != nil {
		for _, fp := range tempFiles {
			fp.Close()
		}
		return nil, err
	}
	return cfReader, nil
}

func (cfReader *ConcurrentReadFile) SeekAndRead(pos int64, buf []byte) (int, error) {
	fp := <-cfReader.fileChan
	defer func() {
		cfReader.fileChan <- fp
	}()
	_, err := fp.Seek(pos, 0)
	if err != nil {
		return 0, err
	}
	return fp.Read(buf)
}

func (cfReader *ConcurrentReadFile) Close() error {
	for i := 0; i < cfReader.size; i++ {
		fp := <-cfReader.fileChan
		fp.Close()
	}
	close(cfReader.fileChan)
	return nil
}

func OpenAsPositionWriteFile(path string) (*PositionWriteFile, error) {
	pfWriter := new(PositionWriteFile)
	pfWriter.fileChan = make(chan *os.File, 1)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	pfWriter.fileChan <- file
	pfWriter.writingPosition = 0
	return pfWriter, nil
}

func (pfWriter *PositionWriteFile) Append(buf []byte) (position uint32, err error) {
	fp := <-pfWriter.fileChan
	defer func() {
		pfWriter.fileChan <- fp
	}()
	position = pfWriter.writingPosition
	_, err = fp.Write(buf)
	if err != nil {
		return 0, err
	}
	pfWriter.writingPosition += uint32(len(buf))
	return position, nil
}

func (pfWriter *PositionWriteFile) SeekAndWrite(pos int64, buf []byte) error {
	fp := <-pfWriter.fileChan
	defer func() {
		pfWriter.fileChan <- fp
	}()
	_, err := fp.Seek(pos, 0)
	if err != nil {
		return err
	}
	defer func() {
		// reset for back
		fp.Seek(int64(pfWriter.writingPosition), 0)
		// ignore Seek result
	}()
	_, err = fp.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (pfWriter *PositionWriteFile) Close() error {
	fp := <-pfWriter.fileChan
	fp.Close()
	close(pfWriter.fileChan)
}

type ReadWriteFile struct {
	path          string
	writer        *PositionWriteFile
	reader        *ConcurrentReadFile
}

func (rwFile *ReadWriteFile) SeekAndRead(position int64, buf []byte) (int, error) {
	return rwFile.reader.SeekAndRead(position, buf)
}

func (rwFile *ReadWriteFile) Close() error {
	defer rwFile.reader.Close()
	defer rwFile.writer.Close()
}
