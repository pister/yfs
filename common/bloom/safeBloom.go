package bloom

type SafeBloomFilter struct {
	targetFilter Filter
	op           chan int
}

func NewSafeBloomFilter(bitLength uint32) Filter {
	targetFilter := NewUnsafeBloomFilter(bitLength)
	safeBloomFilter := new(SafeBloomFilter)
	safeBloomFilter.targetFilter = targetFilter
	safeBloomFilter.op = make(chan int, 1)
	return safeBloomFilter
}

func (filter *SafeBloomFilter) Add(data []byte) {
	filter.op <- 1
	defer func() {
		<- filter.op
	}()
	filter.targetFilter.Add(data)
}

func (filter *SafeBloomFilter) Hit(data []byte) bool {
	filter.op <- 1
	defer func() {
		<- filter.op
	}()
	return filter.targetFilter.Hit(data)
}

func (filter *SafeBloomFilter) GetBitData() ([]byte, uint32) {
	filter.op <- 1
	defer func() {
		<- filter.op
	}()
	return filter.targetFilter.GetBitData()
}