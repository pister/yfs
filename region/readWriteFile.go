package region

import (
	"os"
	"sync/atomic"
	"fmt"
	"io"
)

type ConcurrentReadFile struct {
	fileChan chan *os.File
	size     int
}

type PositionWriteFile struct {
	fileChan        chan *os.File
	writingPosition int64
}


func openAsConcurrentReadFile(path string, concurrentSize int) (*ConcurrentReadFile, error) {
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

func (cfReader *ConcurrentReadFile) SeekAndReadData(pos int64, buf []byte) (int, error) {
	var readCount int
	var err error
	err = cfReader.SeekForReading(pos, func(reader io.Reader) error {
		var e error
		readCount, e = reader.Read(buf)
		return e
	})
	if err != nil {
		return readCount, err
	}
	return readCount, nil
}

func (cfReader *ConcurrentReadFile) SeekForReading(pos int64, callback func(reader io.Reader) error) error {
	if callback == nil {
		return fmt.Errorf("callball must not be nil")
	}
	fp := <-cfReader.fileChan
	defer func() {
		cfReader.fileChan <- fp
	}()
	_, err := fp.Seek(pos, 0)
	if err != nil {
		return err
	}
	return callback(fp)
}

func (cfReader *ConcurrentReadFile) Close() error {
	for i := 0; i < cfReader.size; i++ {
		fp := <-cfReader.fileChan
		fp.Close()
	}
	close(cfReader.fileChan)
	return nil
}

func openAsPositionWriteFile(path string) (*PositionWriteFile, error) {
	pfWriter := new(PositionWriteFile)
	pfWriter.fileChan = make(chan *os.File, 1)
	// here does not use O_APPEND flag because of if it is used, Seek function will be not work.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	pfWriter.writingPosition = fi.Size()
	if pfWriter.writingPosition > 0 {
		// not O_APPEND flag, so seek at end of file
		file.Seek(0, 2)
	}
	pfWriter.fileChan <- file

	return pfWriter, nil
}

func (pfWriter *PositionWriteFile) Append(buf []byte) (writeStartPosition int64, err error) {
	fp := <-pfWriter.fileChan
	defer func() {
		pfWriter.fileChan <- fp
	}()
	writeStartPosition = pfWriter.writingPosition
	_, err = fp.Write(buf)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&pfWriter.writingPosition, int64(len(buf)))
	return writeStartPosition, nil
}

func (pfWriter *PositionWriteFile) Seek(pos int64) (oldPosition int64, err error) {
	fp := <-pfWriter.fileChan
	defer func() {
		pfWriter.fileChan <- fp
	}()
	if pos > pfWriter.writingPosition {
		return 0, fmt.Errorf("seek out of end of file")
	}
	oldPosition = pfWriter.writingPosition
	_, err = fp.Seek(pos, 0)
	if err != nil {
		return 0, err
	}
	pfWriter.writingPosition = pos
	return oldPosition, nil
}

func (pfWriter *PositionWriteFile) UpdateAt(pos int64, buf []byte) error {
	fp := <-pfWriter.fileChan
	defer func() {
		pfWriter.fileChan <- fp
	}()
	if pos + int64(len(buf)) > pfWriter.writingPosition {
		return fmt.Errorf("can not update end of file")
	}
	_, err := fp.Seek(pos, 0)
	if err != nil {
		return err
	}
	// reset for back, ignore Seek result
	defer fp.Seek(int64(pfWriter.writingPosition), 0)
	_, err = fp.Write(buf)
	return err
}

func (pfWriter *PositionWriteFile) Close() error {
	fp := <-pfWriter.fileChan
	fp.Close()
	close(pfWriter.fileChan)
	return nil
}

type ReadWriteFile struct {
	path   string
	writer *PositionWriteFile
	reader *ConcurrentReadFile
}

func OpenReadWriteFile(path string, concurrentSize int) (*ReadWriteFile, error) {
	readWriteFile := new(ReadWriteFile)
	readWriteFile.path = path
	writer, err := openAsPositionWriteFile(path)
	if err != nil {
		return nil, err
	}
	reader, err := openAsConcurrentReadFile(path, concurrentSize)
	if err != nil {
		writer.Close()
		return nil, err
	}
	readWriteFile.writer = writer
	readWriteFile.reader = reader
	return readWriteFile, nil
}

func (rwFile *ReadWriteFile) SeekAndReadData(position int64, buf []byte) (int, error) {
	return rwFile.reader.SeekAndReadData(position, buf)
}

func (rwFile *ReadWriteFile) SeekForReading(pos int64, callback func(reader io.Reader) error) error {
	return rwFile.reader.SeekForReading(pos, callback)
}

func (rwFile *ReadWriteFile) Append(buf []byte) (writeStartPosition int64, err error) {
	return rwFile.writer.Append(buf)
}

func (rwFile *ReadWriteFile) UpdateAt(pos int64, buf []byte) error {
	return rwFile.writer.UpdateAt(pos, buf)
}

func (rwFile *ReadWriteFile) UpdateByteAt(pos int64, b byte) error {
	buf := make([]byte, 1)
	buf[0] = b
	return rwFile.writer.UpdateAt(pos, buf)
}

func (rwFile *ReadWriteFile) GetFileLength() int64 {
	return rwFile.writer.writingPosition
}

func (rwFile *ReadWriteFile) Seek(pos int64) (oldPosition int64, err error) {
	return rwFile.writer.Seek(pos)
}

func (rwFile *ReadWriteFile) Close() error {
	defer rwFile.reader.Close()
	defer rwFile.writer.Close()
	return nil
}
