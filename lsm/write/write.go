package write

import (
	"github.com/pister/yfs/common/lockutil"
	"sync/atomic"
	"unsafe"
)

type Writer struct {
	lsm          unsafe.Pointer
	flushOutLock *lockutil.TryLocker
	dir          string
}

func (writer *Writer) FlushLsm() error {
	if !writer.flushOutLock.TryLock() {
		return nil
	}
	defer writer.flushOutLock.UnLock()

	newLsm, err := NewLsm(writer.dir)
	if err != nil {
		return err
	}
	oldLsm := (*Lsm)(writer.lsm)

	atomic.StorePointer(&writer.lsm, unsafe.Pointer(newLsm))

	go oldLsm.FlushAndClose()

	return nil
}

func (writer *Writer) getCurrentLsm() *Lsm {
	return (*Lsm)(writer.lsm)
}

func (writer *Writer) Put(key []byte, value []byte) error {
	lsm := writer.getCurrentLsm()
	err := lsm.Put(key, value)
	if err != nil {
		return err
	}
	if lsm.NeedFlush() {
		writer.FlushLsm()
	}
	return nil
}

func (writer *Writer) Delete(key []byte) error {
	lsm := writer.getCurrentLsm()
	return lsm.Delete(key)
}
