package write

import (
	"github.com/pister/yfs/common/bytesutil"
	"fmt"
	"github.com/pister/yfs/common/bloom"
	"github.com/pister/yfs/common/ioutil"
	"io"
	"path"
	"strings"
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

	dataIndexStartPosition, err := readFooter(r)
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
	4 - bytes data sum
	8 - bytes ts
	4 - bytes key length
	4 - bytes data length
	bytes for key
	bytes for data
	*/
	filter := bloom.NewUnsafeBloomFilter(bloomBitSize)
	err := r.SeekForReading(int64(dataIndexStartPosition), func(reader io.Reader) error {
		for {
			header := make([]byte, 24)
			if _, err := reader.Read(header); err != nil {
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

func (reader *SSTableReader) GetByKey(key []byte) (*BlockData, error) {
	if !reader.filter.Hit(key) {
		fmt.Println("not exist...")
		return nil, nil
	}
	//reader.reader.
	//reader.reader.Close()
	fmt.Println("key exist!!!")

	return nil, nil
}
