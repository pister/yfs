package sst

import (
	"github.com/pister/yfs/common/bytesutil"
	"fmt"
	"github.com/pister/yfs/common/bloom"
	"github.com/pister/yfs/common/fileutil"
	"io"
	"path"
	"strings"
	"github.com/pister/yfs/common/hashutil"
	"github.com/pister/yfs/common/bitset"
	"github.com/pister/yfs/lsm/base"
)

type SSTableReader struct {
	filter   bloom.Filter
	reader   *fileutil.ConcurrentReadFile
	fileSize int64
}

func OpenSSTableReader(sstFile string) (*SSTableReader, error) {
	return OpenSSTableReaderWithBloomFilter(sstFile, nil)
}

func OpenSSTableReaderWithBloomFilter(sstFile string, filter bloom.Filter) (*SSTableReader, error) {
	_, name := path.Split(sstFile)
	parts := strings.Split(name, "_")
	if len(parts) < 3 {
		return nil, fmt.Errorf("unkonwn sstable files: %s", name)
	}
	level := LevelFromName(parts[1])
	r, err := fileutil.OpenAsConcurrentReadFile(sstFile, concurrentSizeFromLevel(level))
	if err != nil {
		return nil, err
	}
	if r.GetInitFileSize() == 0 {
		r.Close()
		return nil, nil
	}
	if filter == nil {
		filter, err = readBloomFilter(r)
		if err != nil {
			return nil, err
		}
	}
	//data, _ := filter.GetBitData()
	//s := base64util.EncodeBase64ToString(data)
	//fmt.Println(s)
	reader := new(SSTableReader)
	reader.filter = filter
	reader.reader = r
	reader.fileSize = r.GetInitFileSize()
	return reader, nil
}

func readBloomFilter(r *fileutil.ConcurrentReadFile) (bloom.Filter, error) {
	/*
		2 - bytes magic code
		1 - byte not used
		1 - byte block type
		4 - bit set length
		4 - bloom filter data length
		...bytes for bloom filter
	*/
	_, bloomFilterPosition, err := readFooter(r)
	if err != nil {
		return nil, err
	}
	var bitSize uint32 = 0
	var bitBuf []byte
	if err := r.SeekForReading(int64(bloomFilterPosition), func(reader io.Reader) error {
		header := make([]byte, 12)
		if _, err := reader.Read(header); err != nil {
			return err
		}
		if header[0] != bloomFilterMagicCode1 || header[1] != bloomFilterMagicCode2 {
			return fmt.Errorf("bloom filter magic code not match")
		}
		if header[3] != BlockTypeBloomFilter {
			return fmt.Errorf("type not match for bloom filter data")
		}
		bitSize = bytesutil.GetUint32FromBytes(header, 4)
		dataSize := bytesutil.GetUint32FromBytes(header, 8)
		bitBuf = make([]byte, dataSize)
		_, err := reader.Read(bitBuf)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return bloom.NewUnsafeBloomFilterWithBitSize(bitset.NewBitSetWithInitData(bitSize, bitBuf)), nil
}

func readFooter(r *fileutil.ConcurrentReadFile) (uint32, uint32, error) {
	/*
	2 - bytes magic code
	1 - byte not used
	1 - byte block type
	4 - bytes data-index-start-position-Index
	4 - bytes bloom-filter-position
	*/
	initFileSize := r.GetInitFileSize()
	buf := make([]byte, 12)
	_, err := r.SeekAndReadData(initFileSize-12, buf)
	if err != nil {
		return 0, 0, err
	}
	dataIndexStartPosition := bytesutil.GetUint32FromBytes(buf, 4)
	bloomFilterPosition := bytesutil.GetUint32FromBytes(buf, 8)
	if buf[0] != footerMagicCode1 || buf[1] != footerMagicCode2 {
		return 0, 0, fmt.Errorf("footer magic code not match")
	}
	if buf[3] != BlockTypeFooter {
		return 0, 0, fmt.Errorf("footer block type not match")
	}
	return dataIndexStartPosition, bloomFilterPosition, nil
}

func (reader *SSTableReader) Close() error {
	return reader.reader.Close()
}

func (reader *SSTableReader) getDataIndexes() ([]uint32, error) {
	dataIndexStartPosition, _, err := readFooter(reader.reader)
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
			if buf[3] != BlockTypeDataIndex {
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

func ReadDataHeader(reader io.Reader) (*base.BlockDataHeader, error) {
	var blockDataHeader = new(base.BlockDataHeader)
	header := make([]byte, 24)
	if _, err := reader.Read(header); err != nil {
		return nil, err
	}
	blockDataHeader.MagicCode1 = header[0]
	blockDataHeader.MagicCode2 = header[1]
	blockDataHeader.BlockType = header[3]
	blockDataHeader.Deleted = base.DeletedFlag(header[2]) // delete flag
	blockDataHeader.DataSum = bytesutil.GetUint32FromBytes(header, 4)
	blockDataHeader.Ts = bytesutil.GetUint64FromBytes(header, 8)
	keyLength := bytesutil.GetUint32FromBytes(header, 16)
	blockDataHeader.ValueLength = bytesutil.GetUint32FromBytes(header, 20)

	if blockDataHeader.BlockType != BlockTypeData {
		return nil, nil
	}

	if keyLength > base.MaxKeyLen {
		return nil, fmt.Errorf("too big key length")
	}
	if blockDataHeader.ValueLength > base.MaxValueLen {
		return nil, fmt.Errorf("too big value length")
	}
	keyBuf := make([]byte, keyLength)
	if _, err := reader.Read(keyBuf); err != nil {
		return nil, err
	}
	blockDataHeader.Key = keyBuf
	return blockDataHeader, nil
}

func (reader *SSTableReader) getByDataIndexAndCompareByKey(dataIndex uint32, key []byte) (*base.BlockData, KeyCompareResult, error) {
	var blockData = new(base.BlockData)
	var compareResult KeyCompareResult
	if err := reader.reader.SeekForReading(int64(dataIndex), func(reader io.Reader) error {
		dataHeader, err := ReadDataHeader(reader)
		if err != nil {
			return err
		}
		if dataHeader == nil {
			return fmt.Errorf("not found data")
		}
		if dataHeader.MagicCode1 != dataMagicCode1 || dataHeader.MagicCode2 != dataMagicCode2 {
			return fmt.Errorf("data index magic code not match")
		}
		if dataHeader.BlockType != BlockTypeData {
			return fmt.Errorf("block type not match")
		}
		compareResult = KeyCompare(key, dataHeader.Key)
		if compareResult != Equals {
			return nil
		}
		valueBuf := make([]byte, dataHeader.ValueLength)
		if _, err := reader.Read(valueBuf); err != nil {
			return err
		}
		dataSumByCal := hashutil.SumHash32(valueBuf)
		if dataSumByCal != dataHeader.DataSum {
			return fmt.Errorf("sum not match")
		}
		blockData.Value = valueBuf
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

func (reader *SSTableReader) searchByKey(key []byte, dataIndexes []uint32) (*base.BlockData, error) {
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

func (reader *SSTableReader) GetByKey(key []byte) (*base.BlockData, error) {
	if !reader.filter.Hit(key) {
		return nil, nil
	}
	dataIndexes, err := reader.getDataIndexes()
	if err != nil {
		return nil, err
	}
	return reader.searchByKey(key, dataIndexes)
}
