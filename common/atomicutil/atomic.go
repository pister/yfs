package atomicutil

import "sync/atomic"

type AtomicUint64 struct {
	value uint64
}

func NewAtomicUint64(v uint64) *AtomicUint64 {
	return &AtomicUint64{v}
}

func (v *AtomicUint64) Add(other uint64) uint64 {
	return atomic.AddUint64(&v.value, other)
}

func (v *AtomicUint64) Increment() uint64 {
	return v.Add(1)
}

func (v *AtomicUint64) Decrement() uint64 {
	return v.Add(^uint64(0))
}

func (v *AtomicUint64) Get() uint64 {
	return atomic.LoadUint64(&v.value)
}

type AtomicInt64 struct {
	value int64
}

func NewAtomicInt64(v int64) *AtomicInt64 {
	return &AtomicInt64{v}
}

func (v *AtomicInt64) Add(other int64) int64 {
	return atomic.AddInt64(&v.value, other)
}

func (v *AtomicInt64) Increment() int64 {
	return v.Add(1)
}

func (v *AtomicInt64) Decrement() int64 {
	return v.Add(-1)
}

func (v *AtomicInt64) Get() int64 {
	return atomic.LoadInt64(&v.value)
}