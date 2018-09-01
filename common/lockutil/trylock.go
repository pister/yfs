package lockutil

type TryLocker struct {
	locker *tryLockerImpl
}

type tryLockerImpl struct {
	c chan bool
}

func NewTryLocker() *TryLocker {
	tryLocker := new(TryLocker)
	tryLocker.locker = new(tryLockerImpl)
	tryLocker.locker.c = make(chan bool, 1)
	return tryLocker
}

func (locker *TryLocker) Lock() {
	locker.locker.c <- true
}

func (locker *TryLocker) UnLock() {
	<-locker.locker.c
}


func (locker *TryLocker) TryLock() bool {
	select {
	case locker.locker.c <- true:
		return true
	default:
		return false
	}
}

