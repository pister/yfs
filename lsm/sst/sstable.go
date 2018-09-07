package sst

// SSTable format summary
/*
	the sst data format:
	block-data-0
	block-data-1
	block-data-2
	...
	block-data-N

	data-index-0			<- data-index-start-position
	data-index-1
	data-index-2
	...
	data-index-N
	bloom-filter-data 		<- bloom-filter-position
	footer:data-index-start-position, bloom-filter-position

details:

block-data layout:
2 - bytes magic code
1 - byte delete flag
1 - byte block type
4 - bytes data sum
8 - bytes ts
4 - bytes key length
4 - bytes data length
...bytes for key
...bytes for data


data-index layout:
2 - bytes magic code
1 - byte not used
1 - byte block type
4 - bytes dataIndex

bloom-filter-position layout:
2 - bytes magic code
1 - byte not used
1 - byte block type
4 - bit set length
4 - bloom filter data length
...bytes for bloom filter

footer layout:
2 - bytes magic code
1 - byte not used
1 - byte block type
4 - bytes data-index-start-position-Index

*/

const (
	dataMagicCode1        = 'D'
	dataMagicCode2        = 'T'
	dataIndexMagicCode1   = 'I'
	dataIndexMagicCode2   = 'X'
	bloomFilterMagicCode1 = 'B'
	bloomFilterMagicCode2 = 'F'
	footerMagicCode1      = 'F'
	footerMagicCode2      = 'T'

)

const (
	BlockTypeData         = 1
	BlockTypeDataIndex    = 2
	BlockTypeBloomFilter  = 3
	BlockTypeFooter       = 8
)

type SSTableLevel int

const (
	LevelA SSTableLevel = iota
	LevelB
	LevelC
)

func (level SSTableLevel) Name() string {
	switch level {
	case LevelA:
		return "a"
	case LevelB:
		return "b"
	case LevelC:
		return "c"
	default:
		return "a"
	}
}

func LevelFromName(name string) SSTableLevel {
	switch name {
	case "a":
		return LevelA
	case "b":
		return LevelB
	case "c":
		return LevelC
	default:
		return LevelA
	}
}

func bloomBitSizeFromLevel(level SSTableLevel) uint32 {
	switch level {
	case LevelA:
		return 1024 * 1024
	case LevelB:
		return 5 * 1024 * 1024
	case LevelC:
		return 10 * 1024 * 1024
	default:
		return 1024 * 1024
	}
}

func concurrentSizeFromLevel(level SSTableLevel) int {
	switch level {
	case LevelA:
		return 4
	case LevelB:
		return 6
	case LevelC:
		return 10
	default:
		return 4
	}
}