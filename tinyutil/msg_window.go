package tinyutil

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// MsgWindow 是一个消息并行处理的缓冲区窗口，它是基于环形缓冲区，又读取位，写入位，已经确认集合，
// - 写入偏移位，下一个写入的偏移位，写入一个数据后，缓冲区长度+1，当缓冲区长度达到一定程度后，写入将会阻塞，写入位必须有序，且连续。
// - 待确认偏移位列表，被写入的位置将变成待确认，多个待确认偏移位是可以异步，并行确认的。
// - 读取偏移位，下一个读取的偏移位，仅当该偏移位写入数据且确认之后才会变得可读。读取后，缓冲区长度-1
// MsgWindow 通过一个滑动窗口，解决的是有序序列并行处理消息吞吐量的问题
type MsgWindow struct {
	ring     []int64 // 令牌环
	ringLen  int64   // 环长度
	ringMask int64   // 环访问掩码
	wPos     int64   // 下一个写入位置
	rPos     int64   // 下一个读取位置

	cond  *sync.Cond
	mutex *sync.Mutex
}

// New 新建一个缓冲区窗口
// 参数：
// - l 环形缓冲区的长度，必须为2的N次幂
// - nextPos 起始写入/读取的位置，非负数
func New(l int64, nextPos int64) *MsgWindow {
	if (l & (l - 1)) != 0 {
		panic("length must be 2^n")
	}

	if nextPos < 0 {
		panic("start pos invalid")
	}

	var mutex sync.Mutex
	return &MsgWindow{
		ring:     make([]int64, l),
		ringLen:  l,
		ringMask: l - 1,
		cond:     sync.NewCond(&mutex),
		mutex:    &mutex,
		wPos:     nextPos,
		rPos:     nextPos,
	}
}

func (window *MsgWindow) left() int64 {
	return atomic.LoadInt64(&window.wPos) - atomic.LoadInt64(&window.rPos)
}

func (window *MsgWindow) wait(condition func() bool, do func()) {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	for !condition() {
		window.cond.Wait()
	}

	do()
}

func (window *MsgWindow) notifyAll() {
	window.cond.Broadcast()
}

// Write 向窗口写入新的offset，如果长度已经满了，则会block
// 不能使用并行访问
func (window *MsgWindow) Write(offset int64) {
	lastPos := atomic.LoadInt64(&window.wPos)

	// 重复的消息
	if offset < lastPos {
		return
	}

	// 存在数据丢失
	if lastPos != offset {
		panic(fmt.Sprintf("expect write offset:%d got offset:%d", lastPos, offset))
	}

	var (
		wPos int64
		rPos int64
	)

	// 等到有空间可以写入
	window.wait(func() bool {
		rPos = atomic.LoadInt64(&window.rPos)
		wPos = atomic.LoadInt64(&window.wPos)
		return wPos-rPos < window.ringLen
	}, func() {
		window.ring[window.wPos&window.ringMask] = -1
		atomic.StoreInt64(&window.wPos, offset+1)
		window.notifyAll()
	})
}

// ConfirmOffset 标记offset已经确认处理
func (window *MsgWindow) Confirm(offset int64) {
	wPos := atomic.LoadInt64(&window.wPos)
	rPos := atomic.LoadInt64(&window.rPos)

	// 提交必须在窗口范围内
	if offset >= wPos {
		panic(fmt.Sprintf("offset=%d wPos=%d rPos=%d", offset, wPos, rPos))
	}

	if offset < rPos {
		panic(fmt.Sprintf("offset=%d wPos=%d rPos=%d", offset, wPos, rPos))
	}

	window.mutex.Lock()
	defer window.mutex.Unlock()
	window.ring[window.ringMask&offset] = offset
	window.notifyAll()
}

// Read 读取已经confirm的消息
func (window *MsgWindow) Read() int64 {
	// 如果长度不为0，但是下一个应读未确认处理，则继续等待
	nextReadPos := atomic.LoadInt64(&window.rPos)
	window.wait(func() bool {
		// logrus.WithFields(logrus.Fields{
		// 	"window.left()": window.left(),
		// 	"nextReadPos":   nextReadPos,
		// 	"atomic.LoadInt64(&window.ring[nextReadPos&window.ringMask])": atomic.LoadInt64(&window.ring[nextReadPos&window.ringMask]),
		// }).Infoln("Read")

		return window.left() > 0 && atomic.LoadInt64(&window.ring[nextReadPos&window.ringMask]) == nextReadPos
	}, func() {
		atomic.AddInt64(&window.rPos, 1)
		window.notifyAll()
	})

	return nextReadPos
}

func (window *MsgWindow) ReadPos() int64 {
	return atomic.LoadInt64(&window.rPos)
}

func (window *MsgWindow) WritePos() int64 {
	return atomic.LoadInt64(&window.wPos)
}

// Read 读取已经confirm的消息
func (window *MsgWindow) TryRead() (int64, bool) {
	// 如果长度为0，则继续等待
	for window.left() == 0 {
		return -1, false
	}

	// 如果长度不为0，但是下一个应读未确认处理，则继续等待
	window.mutex.Lock()
	defer window.mutex.Unlock()

	nextReadPos := atomic.LoadInt64(&window.rPos)
	confirmed := window.ring[nextReadPos&window.ringMask] == nextReadPos
	if !confirmed {
		return -1, false
	}

	// 移动读取位置
	atomic.AddInt64(&window.rPos, 1)
	window.notifyAll()

	return nextReadPos, true
}
