package region

import (
	"github.com/pister/yfs/naming"
	"fmt"
	"github.com/pister/yfs/common/ioutil"
	"path/filepath"
	"os"
	"path"
	"regexp"
	"strings"
	"strconv"
	"math/rand"
)

const (
	regionGrowSize                 = 4
	regionBlockConcurrentReadSize  = 3
	regionMayGrowThresholdForGroup = 0.2
)

type Region struct {
	regionId   uint16
	rootPath   string
	regionPath string

	nameBlocks map[uint32]*NameBlock  // map[index-block-id]*
	dataBlocks map[uint32]*BlockStore // map[data-block-id]*
}

func NewRegion(regionId uint16, path string) (*Region, error) {
	err := ioutil.MkDirs(path)
	if err != nil {
		return nil, err
	}
	regionPath := fmt.Sprintf("%s%cregion-%d", path, filepath.Separator, regionId)
	err = ioutil.MkDirs(regionPath)
	if err != nil {
		return nil, err
	}

	region := new(Region)
	region.rootPath = path
	region.regionPath = regionPath
	region.regionId = regionId

	dataBlocks, err := initDataBlocks(regionPath)
	if err != nil {
		return nil, err
	}
	region.dataBlocks = dataBlocks
	// TODO init nameBlocks
	return region, nil
}

func initDataBlocks(regionPath string) (map[uint32]*BlockStore, error) {
	blockDataNamePattern, err := regexp.Compile(`block_data_\d+`)
	if err != nil {
		return nil, err
	}
	blockIndexNamePattern, err := regexp.Compile(`block_index_\d+`)
	if err != nil {
		return nil, err
	}
	blockDataNames := make(map[uint32]string)
	blockIndexNames := make(map[uint32]string)
	err = filepath.Walk(regionPath, func(file string, info os.FileInfo, err error) error {
		_, name := path.Split(file)
		if blockDataNamePattern.MatchString(name) {
			post := strings.LastIndex(name, "_")
			blockId, err := strconv.Atoi(name[post+1:])
			if err != nil {
				return err
			}
			blockDataNames[uint32(blockId)] = file
		} else if blockIndexNamePattern.MatchString(name) {
			post := strings.LastIndex(name, "_")
			blockId, err := strconv.Atoi(name[post+1:])
			if err != nil {
				return err
			}
			blockIndexNames[uint32(blockId)] = file
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	blockStores := make(map[uint32]*BlockStore)
	for blockId := range blockDataNames {
		_, exist := blockIndexNames[blockId]
		if !exist {
			continue
		}
		bs, err := OpenBlockStore(regionPath, blockId, regionBlockConcurrentReadSize)
		if err != nil {
			return nil, err
		}
		blockStores[blockId] = bs
	}
	return blockStores, nil
}

func (region *Region) growUp() {
	// TODO
	// async task
}

func (region *Region) getBlockToWrite(data []byte) (*NameBlock, *BlockStore) {
	bestAvailableBlocks := make([]*BlockStore, 0, 4)
	for _, dataBlock := range region.dataBlocks {
		if dataBlock.AvailableRate() > regionMayGrowThresholdForGroup {
			bestAvailableBlocks = append(bestAvailableBlocks, dataBlock)
		}
	}
	if len(bestAvailableBlocks) < regionGrowSize {
		// grow up
		region.growUp()
	}
	i := rand.Int31n(int32(len(bestAvailableBlocks)))
	dataBlock := bestAvailableBlocks[i]

	// TODO nameBlock
	return nil, dataBlock
}

func (region *Region) Add(data []byte) (*naming.Name, error) {
	indexBlock, dataBlock := region.getBlockToWrite(data)
	di, err := dataBlock.Add(data)
	if err != nil {
		return nil, err
	}
	name, err := indexBlock.Add(di)
	if err != nil {
		dataBlock.Delete(di)
		// ignore delete error
		return nil, err
	}
	return name, nil
}

func (region *Region) Get(name *naming.Name) ([]byte, error) {
	indexBlock, exist := region.nameBlocks[name.IndexBlockId]
	if !exist {
		return nil, fmt.Errorf("index block not found")
	}
	bi, exist, err := indexBlock.Get(name)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	dataBlock, exist := region.dataBlocks[bi.blockId]
	if !exist {
		return nil, nil
	}
	return dataBlock.Get(bi)
}

func (region *Region) Delete(name *naming.Name) error {
	indexBlock, exist := region.nameBlocks[name.IndexBlockId]
	if !exist {
		return fmt.Errorf("index block not found")
	}
	bi, exist, err := indexBlock.Get(name)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	// 1, delete for data block
	dataBlock, exist := region.dataBlocks[bi.blockId]
	if !exist {
		return fmt.Errorf("data block not found")
	}
	err = dataBlock.Delete(bi)
	if err != nil {
		return err
	}
	// 2, delete fro index block
	return indexBlock.Delete(name)
}
