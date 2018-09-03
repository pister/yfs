package write

import (
	"github.com/pister/yfs/common/bytesutil"
	"fmt"
	"github.com/pister/yfs/common/bloom"
	"github.com/pister/yfs/common/ioutil"
	"io"
	"path"
	"strings"
	"github.com/pister/yfs/common/hashutil"
)

type SSTableReader struct {
	filter bloom.Filter
	reader *ioutil.ConcurrentReadFile
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

	filter, err := readDataIndexAsFilter(r, bloomBitSizeFromLevel(level))
	if err != nil {
		return nil, err
	}
	reader := new(SSTableReader)
	reader.filter = filter
	reader.reader = r
	return reader, nil
}

func readDataIndexAsFilter(r *ioutil.ConcurrentReadFile, bloomBitSize uint32) (bloom.Filter, error) {
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
	filter := bloom.NewUnsafeBloomFilter(bloomBitSize)
	err := r.SeekForReading(0, func(reader io.Reader) error {
		for {
			header := make([]byte, 24)
			if _, err := reader.Read(header); err != nil {
				return err
			}
			// header[2] // delete flag, not use here
			if header[3] != blockTypeData {
				// end
				return nil
			}
			if header[0] != dataMagicCode1 || header[1] != dataMagicCode2 {
				return fmt.Errorf("data index magic code not match")
			}
			// header[4:8] // dataSum not use here
			// dataSum := bytesutil.GetUint32FromBytes(header, 4)
			// ts := bytesutil.GetUint64FromBytes(header, 8)
			keyLength := bytesutil.GetUint32FromBytes(header, 16)
			valueLength := bytesutil.GetUint32FromBytes(header, 20)

			if keyLength > maxKeyLen {
				return fmt.Errorf("too big key length")
			}
			if valueLength > maxValueLen {
				return fmt.Errorf("too big value length")
			}
			key := make([]byte, keyLength)
			if _, err := reader.Read(key); err != nil {
				return err
			}
			valueBuf := make([]byte, valueLength)
			if _, err := reader.Read(valueBuf); err != nil {
				return err
			}
			filter.Add(key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filter, nil
}

func readFooter(r *ioutil.ConcurrentReadFile) (uint32, error) {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes data-index-start-position-Index
	*/
	initFileSize := r.GetInitFileSize()
	buf := make([]byte, 8)
	_, err := r.SeekAndReadData(initFileSize-8, buf)
	if err != nil {
		return 0, err
	}
	dataIndexStartPosition := bytesutil.GetUint32FromBytes(buf, 4)
	if buf[0] != footerMagicCode1 || buf[1] != footerMagicCode2 {
		return 0, fmt.Errorf("footer magic code not match")
	}
	if buf[3] != blockTypeFooter {
		return 0, fmt.Errorf("footer block type not match")
	}
	return dataIndexStartPosition, nil
}

func (reader *SSTableReader) getDataIndexes() ([]uint32, error) {
	dataIndexStartPosition, err := readFooter(reader.reader)
	if err != nil {
		return nil, err
	}
	dataIndexes := make([]uint32, 0, 256)
	if err := reader.reader.SeekForReading(int64(dataIndexStartPosition), func(reader io.Reader) error {
		/*
			2 - bytes magic code
			1 - byte not used
			1 - byte block type
			4 - bytes dataIndex
			*/
		for {
			buf := make([]byte, 8)
			_, err := reader.Read(buf)
			if err != nil {
				return err
			}
			if buf[3] != blockTypeDataIndex {
				return nil
			}
			if buf[0] != dataIndexMagicCode1 || buf[1] != dataIndexMagicCode2 {
				return fmt.Errorf("data index magic code not match")
			}
			dataIndex := bytesutil.GetUint32FromBytes(buf, 4)
			dataIndexes = append(dataIndexes, dataIndex)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return dataIndexes, nil
}

type KeyCompareResult int

const (
	Less    KeyCompareResult = -1
	Equals  KeyCompareResult = 0
	Greater KeyCompareResult = 1
)

func KeyCompare(me, other []byte) KeyCompareResult {
	lenMe := len(me)
	lenOther := len(other)
	if lenMe == 0 && lenOther == 0 {
		return Equals
	}
	if lenMe == 0 {
		return Less
	}
	if lenOther == 0 {
		return Greater
	}
	minLen := 0
	if lenMe > lenOther {
		minLen = lenOther
	} else {
		minLen = lenMe
	}
	for i := 0; i < minLen; i++ {
		if me[i] > other[i] {
			return Greater
		} else if me[i] < other[i] {
			return Less
		}
		// continue next byte
	}
	if lenMe == lenOther {
		return Equals
	}
	if lenMe > lenOther {
		return Greater
	} else {
		return Less
	}
}

func (reader *SSTableReader) getByDataIndexAndCompareByKey(dataIndex uint32, key []byte) (*BlockData, KeyCompareResult, error) {
	var blockData = new(BlockData)
	var compareResult KeyCompareResult
	if err := reader.reader.SeekForReading(int64(dataIndex), func(reader io.Reader) error {
		header := make([]byte, 24)
		if _, err := reader.Read(header); err != nil {
			return err
		}
		if header[0] != dataMagicCode1 || header[1] != dataMagicCode2 {
			return fmt.Errorf("data index magic code not match")
		}
		if header[3] != blockTypeData {
			return fmt.Errorf("block type not match")
		}
		blockData.deleted = deletedFlag(header[2]) // delete flag
		dataSumFromBuf := bytesutil.GetUint32FromBytes(header, 4)
		blockData.ts = bytesutil.GetUint64FromBytes(header, 8)
		keyLength := bytesutil.GetUint32FromBytes(header, 16)
		valueLength := bytesutil.GetUint32FromBytes(header, 20)

		if keyLength > maxKeyLen {
			return fmt.Errorf("too big key length")
		}
		if valueLength > maxValueLen {
			return fmt.Errorf("too big value length")
		}
		keyBuf := make([]byte, keyLength)
		if _, err := reader.Read(keyBuf); err != nil {
			return err
		}
		compareResult = KeyCompare(key, keyBuf)
		if compareResult != Equals {
			return nil
		}
		valueBuf := make([]byte, valueLength)
		if _, err := reader.Read(valueBuf); err != nil {
			return err
		}
		dataSumByCal := hashutil.SumHash32(valueBuf)
		if dataSumByCal != dataSumFromBuf {
			return fmt.Errorf("sum not match")
		}
		blockData.value = valueBuf
		return nil
	}); err != nil {
		return nil, compareResult, err
	}
	if compareResult != Equals {
		return nil, compareResult, nil
	} else {
		return blockData, compareResult, nil
	}
}

func (reader *SSTableReader) searchByKey(key []byte, dataIndexes []uint32) (*BlockData, error) {
	lowBound := 0
	upBound := len(dataIndexes)
	var pos = -1
	for {
		if upBound <= lowBound {
			return nil, nil
		}
		newPos := (upBound + lowBound) / 2
		if newPos == pos {
			return nil, nil
		}
		pos = newPos
		dataIndex := dataIndexes[pos]
		blockData, compareResult, err := reader.getByDataIndexAndCompareByKey(dataIndex, key)
		if err != nil {
			return nil, err
		}
		if compareResult == Equals {
			return blockData, nil
		}
		if compareResult == Greater {
			if lowBound < pos {
				lowBound = pos
			} else {
				lowBound += 1
			}
		} else {
			if upBound > pos {
				upBound = pos
			} else {
				upBound -= 1
			}
		}
	}
}

func (reader *SSTableReader) GetByKey(key []byte) (*BlockData, error) {
	if !reader.filter.Hit(key) {
		return nil, nil
	}
	dataIndexes, err := reader.getDataIndexes()
	if err != nil {
		return nil, err
	}
	return reader.searchByKey(key, dataIndexes)
}
