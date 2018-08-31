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
	"github.com/pister/yfs/common/maputil"
)

const (
	regionDataGrowSize                 = 4
	regionNameGrowSize                 = 2
	regionDataBlockConcurrentReadSize  = 3
	regionNameBlockConcurrentReadSize  = 4
	regionDataMayGrowThresholdForGroup = 0.2
	regionNameMayGrowThresholdForGroup = 0.2
)

type Region struct {
	regionId   uint16
	rootPath   string
	regionPath string

	nameBlocks *maputil.SafeMap // map[index-block-id]*
	dataBlocks *maputil.SafeMap // map[data-block-id]*
}

func OpenRegion(regionId uint16, path string) (*Region, error) {
	err := ioutil.MkDirs(path)
	if err != nil {
		return nil, err
	}
	regionPath := fmt.Sprintf("%s%cregion-%d", path, filepath.Separator, regionId)
	err = ioutil.MkDirs(regionPath)
	if err != nil {
		return nil, err
	}

	dataBlocks, err := initDataBlocks(regionPath)
	if err != nil {
		return nil, err
	}
	nameBlocks, err := initNameBlocks(regionId, regionPath)
	if err != nil {
		dataBlocks.Foreach(func(key interface{}, value interface{}) (stop bool) {
			value.(*NameBlock).Close()
			return false
		})
		return nil, err
	}

	region := new(Region)
	region.rootPath = path
	region.regionPath = regionPath
	region.regionId = regionId

	region.dataBlocks = dataBlocks
	region.nameBlocks = nameBlocks

	return region, nil
}

func initNameBlocks(regionId uint16, regionPath string) (*maputil.SafeMap, error) {
	nameBlocNamePattern, err := regexp.Compile(`name_block_\d+`)
	if err != nil {
		return nil, err
	}
	nameBlockFiles := maputil.NewSafeMap()
	err = filepath.Walk(regionPath, func(file string, info os.FileInfo, err error) error {
		_, name := path.Split(file)
		if nameBlocNamePattern.MatchString(name) {
			post := strings.LastIndex(name, "_")
			blockId, err := strconv.Atoi(name[post+1:])
			if err != nil {
				return err
			}
			nameBlockFile, err := OpenNameBlock(regionId, regionPath, uint32(blockId), regionNameBlockConcurrentReadSize)
			if err != nil {
				return err
			}
			nameBlockFiles.Put(uint32(blockId), nameBlockFile)
		}
		return nil
	})
	if err != nil {
		nameBlockFiles.Foreach(func(key interface{}, value interface{}) (stop bool) {
			value.(*NameBlock).Close()
			return false
		})
		return nil, err
	}
	if nameBlockFiles.Length() <= 0 {
		// create init data
		for blockId := 0; blockId < regionNameGrowSize; blockId++ {
			nameBlock, err := OpenNameBlock(regionId, regionPath, uint32(blockId), regionDataBlockConcurrentReadSize)
			if err != nil {
				return nil, err
			}
			nameBlockFiles.Put(blockId, nameBlock)
		}
	}
	return nameBlockFiles, nil
}

func initDataBlocks(regionPath string) (*maputil.SafeMap, error) {
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
	dataBlocks := maputil.NewSafeMap()
	for blockId := range blockDataNames {
		_, exist := blockIndexNames[blockId]
		if !exist {
			continue
		}
		bs, err := OpenDataBlock(regionPath, blockId, regionDataBlockConcurrentReadSize)
		if err != nil {
			return nil, err
		}
		dataBlocks.Put(blockId, bs)
	}
	if dataBlocks.Length() <= 0 {
		// create init data
		for blockId := 0; blockId < regionDataGrowSize; blockId++ {
			dataBlock, err := OpenDataBlock(regionPath, uint32(blockId), regionDataBlockConcurrentReadSize)
			if err != nil {
				return nil, err
			}
			dataBlocks.Put(blockId, dataBlock)
		}
	}
	return dataBlocks, nil
}

func (region *Region) growUpDataBlock() {
	// TODO
	// async task
}

func (region *Region) growUpNameBlock() {
	// TODO
	// async task
}

func (region *Region) getNameBlockToWrite() *NameBlock {
	bestAvailableBlocks := make([]*NameBlock, 0, 2)
	region.nameBlocks.Foreach(func(key interface{}, value interface{}) (stop bool) {
		nameBlock := value.(*NameBlock)
		if nameBlock.AvailableRate() > regionNameMayGrowThresholdForGroup {
			bestAvailableBlocks = append(bestAvailableBlocks, nameBlock)
		}
		return false
	})
	if len(bestAvailableBlocks) < regionNameGrowSize {
		// grow up
		region.growUpNameBlock()
	}
	nLen := int32(len(bestAvailableBlocks))
	return bestAvailableBlocks[rand.Int31n(nLen)]
}

func (region *Region) getDataBlockToWrite(data []byte) (*DataBlock) {
	bestAvailableBlocks := make([]*DataBlock, 0, 4)
	region.dataBlocks.Foreach(func(key interface{}, value interface{}) (stop bool) {
		dataBlock := value.(*DataBlock)
		if dataBlock.AvailableRate() > regionDataMayGrowThresholdForGroup {
			bestAvailableBlocks = append(bestAvailableBlocks, dataBlock)
		}
		return false
	})

	if len(bestAvailableBlocks) < regionDataGrowSize {
		// grow up
		region.growUpDataBlock()
	}
	nLen := int32(len(bestAvailableBlocks))
	dataBlock := bestAvailableBlocks[rand.Int31n(nLen)]
	return dataBlock
}

func (region *Region) Close() error {
	defer func() {
		region.dataBlocks.Foreach(func(key interface{}, value interface{}) (stop bool) {
			value.(*DataBlock).Close()
			return false
		})
	}()
	defer func() {
		region.nameBlocks.Foreach(func(key interface{}, value interface{}) (stop bool) {
			value.(*NameBlock).Close()
			return false
		})
	}()
	return nil
}

func (region *Region) Add(data []byte) (*naming.Name, error) {
	dataBlock := region.getDataBlockToWrite(data)
	nameBlock := region.getNameBlockToWrite()
	di, err := dataBlock.Add(data)
	if err != nil {
		return nil, err
	}
	name, err := nameBlock.Add(di)
	if err != nil {
		dataBlock.Delete(di)
		// ignore delete error
		return nil, err
	}
	return name, nil
}

func (region *Region) Get(name *naming.Name) ([]byte, error) {
	nameBlock, exist := region.nameBlocks.Get(name.NameBlockId)
	if !exist {
		return nil, fmt.Errorf("index block not found")
	}
	bi, exist, err := nameBlock.(*NameBlock).Get(name)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	dataBlock, exist := region.dataBlocks.Get(bi.blockId)
	if !exist {
		return nil, nil
	}
	return dataBlock.(*DataBlock).Get(bi)
}

func (region *Region) Delete(name *naming.Name) error {
	nameBlock, exist := region.nameBlocks.Get(name.NameBlockId)
	if !exist {
		return nil
	}
	bi, exist, err := nameBlock.(*NameBlock).Get(name)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	// 1, delete for data block
	dataBlock, exist := region.dataBlocks.Get(bi.blockId)
	if !exist {
		return nil
	}
	err = dataBlock.(*DataBlock).Delete(bi)
	if err != nil {
		return err
	}
	// 2, delete fro index block
	return nameBlock.(*NameBlock).Delete(name)
}
