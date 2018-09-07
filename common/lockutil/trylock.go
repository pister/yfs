package lockutil

import "sync"

type TryLocker interface {
	sync.Locker
	TryLock() bool
}

type ChanTryLocker struct {
	locker *tryLockerImpl
}

type tryLockerImpl struct {
	c chan bool
}

func NewTryLocker() *ChanTryLocker {
	tryLocker := new(ChanTryLocker)
	tryLocker.locker = new(tryLockerImpl)
	tryLocker.locker.c = make(chan bool, 1)
	return tryLocker
}

func (locker *ChanTryLocker) Lock() {
	locker.locker.c <- true
}

func (locker *ChanTryLocker) Unlock() {
	<-locker.locker.c
}

func (locker *ChanTryLocker) TryLock() bool {
	select {
	case locker.locker.c <- true:
		return true
	default:
		return false
	}
}
