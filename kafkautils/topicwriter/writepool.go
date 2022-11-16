package topicwriter

import (
	"sync"
	"time"
)

type WriterPool struct {
	idles map[*Writer]struct{}

	mutex     sync.Mutex
	brokers   []string
	topic     string
	partition int64
}

func NewWritePool(brokers []string, topic string, partition int64) *WriterPool {
	return &WriterPool{
		idles:     make(map[*Writer]struct{}),
		brokers:   brokers,
		topic:     topic,
		partition: partition,
	}
}

func (pool *WriterPool) clearIdles() {
	for idleWriter := range pool.idles {
		// 清理使用超时
		if time.Since(idleWriter.lastWriteTime) > time.Minute*9 {
			idleWriter.close()
			delete(pool.idles, idleWriter)
		}
	}
}

func (pool *WriterPool) Get() *Writer {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// 清理
	pool.clearIdles()

	// 随机选择一个
	for writer := range pool.idles {
		return writer
	}

	writer := &Writer{
		brokers:   pool.brokers,
		topic:     pool.topic,
		partition: int(pool.partition),
	}

	return writer
}

func (pool *WriterPool) Put(writer *Writer) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	pool.idles[writer] = struct{}{}
}
