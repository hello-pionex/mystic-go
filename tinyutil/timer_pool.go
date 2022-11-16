package tinyutil

import (
	"sync"
	"time"
)

type TimerPool struct {
	sync.Pool
}

var defaultTimerPool = NewTimerPool()

func NewTimerPool() *TimerPool {
	return &TimerPool{
		Pool: sync.Pool{
			New: func() interface{} {
				return time.NewTimer(time.Second * 600)
			},
		},
	}
}

func (pool *TimerPool) NewTimer(d time.Duration) *time.Timer {
	timer := pool.Pool.Get().(*time.Timer)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	timer.Reset(d)
	return timer
}

func (pool *TimerPool) FreeTimer(t *time.Timer) {
	t.Stop()
	pool.Pool.Put(t)
}

func NewTimer(d time.Duration) *time.Timer {
	return defaultTimerPool.NewTimer(d)
}

func FreeTimer(t *time.Timer) {
	defaultTimerPool.FreeTimer(t)
}
