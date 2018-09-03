package write

import (
	"os"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
	"fmt"
	"path/filepath"
	"github.com/pister/yfs/common/bloom"
	"github.com/pister/yfs/common/ioutil"
	"github.com/pister/yfs/common/maputil"
	"io"
	"path"
	"strings"
)

// SSTable format
/*
	the sst data format:
	block-data-0
	block-data-1
	block-data-2
	...
	block-data-N
	data-index-0    <- data-index-start-position
	data-index-1
	data-index-2
	....
	key-data-index-N
	key-index-0			<- key-index-start-position
	key-index-1
	key-index-2
	...
	key-index-N
	footer:data-index-start-position,key-index-start-position
*/

const (
	dataMagicCode1      = 'D'
	dataMagicCode2      = 'T'
	dataIndexMagicCode1 = 'K'
	dataIndexMagicCode2 = 'Y'
	keyIndexMagicCode1  = 'I'
	keyIndexMagicCode2  = 'N'
	footerMagicCode1    = 'F'
	footerMagicCode2    = 'T'
	blockTypeData       = 1
	blockTypeDataIndex  = 2
	blockTypeKeyIndex   = 3
	blockTypeFooter     = 4
)

type SSTableLevel int

const (
	sstLevelA SSTableLevel = iota
	sstLevelB
	sstLevelC
)

func (level SSTableLevel) Name() string {
	switch level {
	case sstLevelA:
		return "a"
	case sstLevelB:
		return "b"
	case sstLevelC:
		return "c"
	default:
		return "a"
	}
}

func LevelFromName(name string) SSTableLevel {
	switch name {
	case "a":
		return sstLevelA
	case "b":
		return sstLevelB
	case "c":
		return sstLevelC
	default:
		return sstLevelA
	}
}


type SSTableWriter struct {
	file         *os.File
	position     uint32
	fileName     string
	tempFileName string
}

type SSTableReader struct {
	filter bloom.Filter
	reader *ioutil.ConcurrentReadFile
}


func bloomBitSizeFromLevel(level SSTableLevel) uint32 {
	switch level {
	case sstLevelA:
		return 20 * 1024
	case sstLevelB:
		return 100 * 1024
	case sstLevelC:
		return 1024 * 1024
	default:
		return 20 * 1024
	}
}

func concurrentSizeFromLevel(level SSTableLevel) int {
	switch level {
	case sstLevelA:
		return 3
	case sstLevelB:
		return 4
	case sstLevelC:
		return 5
	default:
		return 3
	}
}


func OpenSSTableReader(sstFile string) (*SSTableReader, error) {
	_, name := path.Split(sstFile)
	parts := strings.Split(name, "_")
	if len(parts) < 3 {
		return nil, fmt.Errorf("unkonwn sstable files: %s", name)
	}
	level := LevelFromName(parts[1])
	r, err := ioutil.OpenAsConcurrentReadFile(sstFile, concurrentSizeFromLevel(level))
	if err != nil {
		return nil, err
	}

	dataIndexStartPosition, _, err := readFooter(r)
	if err != nil {
		return nil, err
	}
	filter, err := readDataIndexAsFilter(dataIndexStartPosition, r, bloomBitSizeFromLevel(level))
	if err != nil {
		return nil, err
	}
	reader := new(SSTableReader)
	reader.filter = filter
	reader.reader = r
	return reader, nil
}

func readDataIndexAsFilter(dataIndexStartPosition uint32, r *ioutil.ConcurrentReadFile, bloomBitSize uint32) (bloom.Filter, error) {
	/*
	2 - bytes magic code
	1 - byte delete flag
	1 - byte block type
	4 - bytes dataIndex
	4 - bytes key length
	bytes for key
	*/
	filter := bloom.NewUnsafeBloomFilter(bloomBitSize)
	err := r.SeekForReading(int64(dataIndexStartPosition), func(reader io.Reader) error {
		for {
			header := make([]byte, 12)
			_, err := reader.Read(header)
			if err != nil {
				return err
			}
			if header[0] != dataIndexMagicCode1 || header[1] != dataIndexMagicCode2 {
				return fmt.Errorf("data index magic code not match")
			}
			// header[2] // delete flag, not use here
			if header[3] != blockTypeDataIndex {
				// end
				return nil
			}
			// header[4:8] // dataIndex not use here
			keyLen := bytesutil.GetUint32FromBytes(header, 8)
			if keyLen > maxKeyLen {
				return fmt.Errorf("too big key length")
			}
			key := make([]byte, keyLen)
			filter.Add(key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filter, nil
}

func readFooter(r *ioutil.ConcurrentReadFile) (uint32, uint32, error) {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes key-Index
	4 - bytes key-index-start-position-Index
	*/
	initFileSize := r.GetInitFileSize()
	buf := make([]byte, 12)
	_, err := r.SeekAndReadData(initFileSize-12, buf)
	if err != nil {
		return 0, 0, err
	}
	dataIndexStartPosition := bytesutil.GetUint32FromBytes(buf, 4)
	keyIndexStartPosition := bytesutil.GetUint32FromBytes(buf, 8)
	if buf[0] != footerMagicCode1 || buf[1] != footerMagicCode2 {
		return 0, 0, fmt.Errorf("footer magic code not match")
	}
	if buf[3] != blockTypeFooter {
		return 0, 0, fmt.Errorf("footer block type not match")
	}
	return dataIndexStartPosition, keyIndexStartPosition, nil
}

func (reader *SSTableReader) GetByKey(key []byte) (*BlockData, error) {
	if !reader.filter.Hit(key) {
		return nil, nil
	}
	//reader.reader.
	//reader.reader.Close()

	return nil, nil
}

func NewSSTableWriter(dir string, level SSTableLevel, ts int64) (*SSTableWriter, error) {
	ssTableWriter := new(SSTableWriter)
	fileName := fmt.Sprintf("%s%c%s_%s_%d", dir, filepath.Separator, "sst", level.Name(), ts)
	tempFileName := fileName + "_tmp"
	file, err := os.OpenFile(tempFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	ssTableWriter.fileName = fileName
	ssTableWriter.tempFileName = tempFileName
	ssTableWriter.file = file
	ssTableWriter.position = 0
	return ssTableWriter, nil
}

func (writer *SSTableWriter) write(buf []byte) (uint32, error) {
	position := writer.position
	_, err := writer.file.Write(buf)
	if err != nil {
		return 0, err
	}
	writer.position += uint32(len(buf))
	return position, nil
}

func (writer *SSTableWriter) writeFooter(dataIndexStartPosition uint32, keyIndexStartPosition uint32) error {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes key-Index
	4 - bytes key-index-start-position-Index
	*/
	buf := make([]byte, 12)
	buf[0] = footerMagicCode1
	buf[1] = footerMagicCode2
	buf[2] = 0
	buf[3] = blockTypeFooter
	bytesutil.CopyUint32ToBytes(dataIndexStartPosition, buf, 4)
	bytesutil.CopyUint32ToBytes(keyIndexStartPosition, buf, 8)
	_, err := writer.write(buf)
	return err
}

func (writer *SSTableWriter) writeKeyIndex(keyIndex uint32) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes key-Index
	*/
	buf := make([]byte, 8)
	buf[0] = keyIndexMagicCode1
	buf[1] = keyIndexMagicCode2
	buf[2] = 0
	buf[3] = blockTypeKeyIndex
	bytesutil.CopyUint32ToBytes(keyIndex, buf, 4)
	return writer.write(buf)
}

func (writer *SSTableWriter) writeDataIndex(key []byte, deleted deletedFlag, dataIndex uint32) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte delete flag
	1 - byte block type
	4 - bytes dataIndex
	4 - bytes key length
	bytes for key
	*/
	buf := make([]byte, 12+len(key))
	buf[0] = dataIndexMagicCode1
	buf[1] = dataIndexMagicCode2
	buf[2] = byte(deleted)
	buf[3] = blockTypeDataIndex
	bytesutil.CopyUint32ToBytes(dataIndex, buf, 4)
	bytesutil.CopyUint32ToBytes(uint32(len(key)), buf, 8)
	bytesutil.CopyDataToBytes(key, 0, buf, 12, len(key))
	return writer.write(buf)
}

func (writer *SSTableWriter) writeData(key []byte, data *BlockData) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte delete flag
	1 - byte block type
	4 - bytes data sum
	8 - bytes ts
	4 - bytes key length
	4 - bytes data length
	bytes for key
	bytes for data
	*/
	headerAndKey := make([]byte, 24+len(key))
	headerAndKey[0] = dataMagicCode1
	headerAndKey[1] = dataMagicCode2
	headerAndKey[2] = byte(data.deleted)
	headerAndKey[3] = blockTypeData
	dataSum := hashutil.SumHash32(data.value)
	bytesutil.CopyUint32ToBytes(dataSum, headerAndKey, 4)
	bytesutil.CopyUint64ToBytes(data.ts, headerAndKey, 8)
	bytesutil.CopyUint32ToBytes(uint32(len(key)), headerAndKey, 16)
	bytesutil.CopyUint32ToBytes(uint32(len(data.value)), headerAndKey, 20)
	bytesutil.CopyDataToBytes(key, 0, headerAndKey, 24, len(key))
	dataIndex, err := writer.write(headerAndKey)
	if err != nil {
		return 0, nil
	}
	_, err = writer.write(data.value)
	if err != nil {
		return 0, nil
	}
	return dataIndex, nil
}

func (writer *SSTableWriter) Close() error {
	return writer.file.Close()
}

func (writer *SSTableWriter) Commit() error {
	if err := os.Rename(writer.tempFileName, writer.fileName); err != nil {
		return err
	}
	return nil
}

func (writer *SSTableWriter) WriteMemMap(memMap *maputil.SafeTreeMap) error {
	dataLength := memMap.Length()
	bloomFilter := bloom.NewUnsafeBloomFilter(100 * 1024)

	var err error
	// 1, write data
	dataIndexes := make([]*dataIndex, 0, dataLength)
	memMap.Foreach(func(key []byte, value interface{}) bool {
		data := value.(*BlockData)
		index, e := writer.writeData(key, data)
		if e != nil {
			err = e
			return true
		}
		bloomFilter.Add(key)
		dataIndexes = append(dataIndexes, &dataIndex{key, data.deleted, index})
		return false
	})
	if err != nil {
		return err
	}
	// 2, write data index
	keyIndexes := make([]uint32, 0, dataLength)
	for _, di := range dataIndexes {
		keyIndex, err := writer.writeDataIndex(di.key, di.deleted, di.dataIndex)
		if err != nil {
			return err
		}
		keyIndexes = append(keyIndexes, keyIndex)
	}
	dataIndexStartPosition := keyIndexes[0]
	var keyIndexStartPosition uint32 = 0
	// 3, write key index
	for _, keyIndex := range keyIndexes {
		p, err := writer.writeKeyIndex(keyIndex)
		if err != nil {
			return err
		}
		if keyIndexStartPosition == 0 {
			keyIndexStartPosition = p
		}
	}
	// 4, writer footer
	if err := writer.writeFooter(dataIndexStartPosition, keyIndexStartPosition); err != nil {
		return err
	}
	return nil
}
