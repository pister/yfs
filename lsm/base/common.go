package base

import (
	"regexp"
	"time"
	"fmt"
	"path/filepath"
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

type ReaderTracker struct {
	FileName    string
	BloomHit    bool
	SearchCount int
}

func (readerTracker ReaderTracker) String() string {
	_, file := filepath.Split(readerTracker.FileName)
	return fmt.Sprintf("<%s - bloom:%v, searchCount:%d>", file, readerTracker.BloomHit, readerTracker.SearchCount)
}

type GetTrackInfo struct {
	EscapeInMillisecond int64
	ReaderTrackers      []ReaderTracker
	InMem               bool
}

func (trackInfo *GetTrackInfo) String() string {
	return fmt.Sprintf("{escape:%d, mem:%v, readers:%v}", trackInfo.EscapeInMillisecond, trackInfo.InMem, trackInfo.ReaderTrackers)
}
