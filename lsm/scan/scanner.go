package scan

import "github.com/pister/yfs/lsm"

type Scanner interface {
	Next() ([]byte, bool, error)
}

type LsmScanner struct {
	lsm     *lsm.Lsm
	current []byte
	start   []byte
	end     []byte
}

func NewLsmScanner(lsm *lsm.Lsm, start, end []byte) (*LsmScanner, error) {
	return nil, nil
}

func (scanner *LsmScanner) Next() ([]byte, bool, error) {

	return nil, false, nil
}
