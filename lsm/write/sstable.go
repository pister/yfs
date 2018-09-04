package write


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

	footer:data-index-start-position

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

footer layout:
2 - bytes magic code
1 - byte not used
1 - byte block type
4 - bytes data-index-start-position-Index

*/


const (
	dataMagicCode1      = 'D'
	dataMagicCode2      = 'T'
	dataIndexMagicCode1 = 'I'
	dataIndexMagicCode2 = 'X'
	footerMagicCode1    = 'F'
	footerMagicCode2    = 'T'
	blockTypeData       = 1
	blockTypeDataIndex  = 2
	blockTypeFooter     = 3
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
		return 4
	case sstLevelB:
		return 6
	case sstLevelC:
		return 10
	default:
		return 4
	}
}
