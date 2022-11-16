package tinyutil

import "sync/atomic"

type Int64 struct {
	v int64
}

func (num *Int64) Load() int64 {
	return atomic.LoadInt64(&num.v)
}

func (num *Int64) Add(n int64) int64 {
	return atomic.AddInt64(&num.v, n)
}

func (num *Int64) Swap(n int64) int64 {
	return atomic.SwapInt64(&num.v, 0)
}
