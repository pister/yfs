package write


// SSTable format
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
		return 3
	case sstLevelB:
		return 4
	case sstLevelC:
		return 5
	default:
		return 3
	}
}
