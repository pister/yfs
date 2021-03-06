package hashutil

func SumHash32(data []byte) uint32 {
	var s uint32 = 0
	for _, d := range data {
		s = s*17 + 13*uint32(d)
	}
	return s
}

func SumHash16(data []byte) uint16 {
	var s uint16 = 0
	for _, d := range data {
		s = s*13 + 7*uint16(d)
	}
	return s
}

func SumHash8(data []byte) uint8 {
	var s uint8 = 0
	for _, d := range data {
		s = s*7 + 17*d
	}
	return s
}
