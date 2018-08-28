package region

import (
	"github.com/pister/yfs/naming"
	"github.com/pister/yfs/utils"
	"fmt"
	"github.com/pister/yfs/sysio"
	"path/filepath"
)

type Region struct {
	writingIndexBlock []*IndexBlock
	writingDataBlock  []*DataBlock
	allIndexBlocks    map[uint32]*IndexBlock // map[index-block-id]*
	allDataBlocks     map[uint32]*DataBlock  // map[data-block-id]*
	regionId          uint16
	rootPath          string
	regionPath        string
}

func NewRegion(regionId uint16, path string) (*Region, error) {
	err := sysio.MkDirs(path)
	if err != nil {
		return nil, err
	}
	regionPath := fmt.Sprintf("%s%cregion-%d", path, filepath.Separator, regionId)
	err = sysio.MkDirs(regionPath)
	if err != nil {
		return nil, err
	}

	region := new(Region)
	region.rootPath = path
	region.regionPath = regionPath
	return region, nil
}

func (region *Region) getBlockToWrite(data []byte) (*IndexBlock, *DataBlock) {
	sumHash := utils.SumHash32(data)
	indexBlock := region.writingIndexBlock[sumHash%uint32(len(region.writingIndexBlock))]
	dataBlock := region.writingDataBlock[sumHash%uint32(len(region.writingDataBlock))]
	return indexBlock, dataBlock
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
	indexBlock, exist := region.allIndexBlocks[name.IndexBlockId]
	if !exist {
		return nil, fmt.Errorf("index block not found")
	}
	bi, err := indexBlock.Get(name)
	if err != nil {
		return nil, err
	}
	dataBlock, exist := region.allDataBlocks[bi.blockId]
	if !exist {
		return nil, fmt.Errorf("data block not found")
	}
	return dataBlock.Get(bi)
}

func (region *Region) Delete(name *naming.Name) error {
	indexBlock, exist := region.allIndexBlocks[name.IndexBlockId]
	if !exist {
		return fmt.Errorf("index block not found")
	}
	bi, err := indexBlock.Get(name)
	if err != nil {
		return err
	}
	// 1, delete for data block
	dataBlock, exist := region.allDataBlocks[bi.blockId]
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
