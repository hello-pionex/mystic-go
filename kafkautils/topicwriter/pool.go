package topicwriter

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

type WriteEventsPool struct {
	pool  *sync.Pool
	count int
}

func (p *WriteEventsPool) Get() []*WriteEvent {
	return *(p.pool.Get().(*[]*WriteEvent))
}

func (p *WriteEventsPool) Put(v []*WriteEvent) {
	if len(v) > p.count {
		return
	}

	v = v[:0]

	p.pool.Put(&v)
}

func NewWriteEventsPool(count int) *WriteEventsPool {
	return &WriteEventsPool{
		pool: &sync.Pool{
			New: func() interface{} {
				slice := make([]*WriteEvent, 0, count)
				return &slice
			},
		},
		count: count,
	}
}

type MessageSlicePool struct {
	pool  *sync.Pool
	count int
}

func (p *MessageSlicePool) Get() []kafka.Message {
	return *(p.pool.Get().(*[]kafka.Message))
}

func (p *MessageSlicePool) Put(v []kafka.Message) {
	if len(v) > p.count {
		return
	}
	v = v[:0]

	p.pool.Put(&v)
}

func NewMessageSlicePool(count int) *MessageSlicePool {
	return &MessageSlicePool{
		pool: &sync.Pool{
			New: func() interface{} {
				slice := make([]kafka.Message, 0, count)
				return &slice
			},
		},
		count: count,
	}
}
