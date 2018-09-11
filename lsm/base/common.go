package base

import (
	"regexp"
	"time"
)

type TsFileName struct {
	PathName string
	Ts       int64
}

type SSTFileSlice []TsFileName

func (sf SSTFileSlice) Len() int {
	return len(sf)
}

func (sf SSTFileSlice) Less(i, j int) bool {
	return sf[i].Ts > sf[j].Ts
}

func (sf SSTFileSlice) Swap(i, j int) {
	tmp := sf[i]
	sf[i] = sf[j]
	sf[j] = tmp
}

var SSTNamePattern *regexp.Regexp

func init() {
	p, err := regexp.Compile(`^sst_\d+_\d+$`)
	if err != nil {
		panic(err)
	}
	SSTNamePattern = p
}

type DeletedFlag byte

const (
	Normal  DeletedFlag = 0
	Deleted             = 1
)

const (
	MaxKeyLen   = 2 * 1024
	MaxValueLen = 20 * 1024 * 1024
)

const (
	MaxMemData = 2 * 1024 * 1024
)

type BlockData struct {
	Deleted DeletedFlag
	Ts      uint64
	Value   []byte
}

type BlockDataHeader struct {
	MagicCode1  byte
	MagicCode2  byte
	Deleted     DeletedFlag
	BlockType   byte
	DataSum     uint32
	ValueLength uint32
	Ts          uint64
	Key         []byte
}

type DataIndex struct {
	Key       []byte
	DataIndex uint32
}

func GetCurrentTs() int64 {
	return time.Now().UnixNano()
}