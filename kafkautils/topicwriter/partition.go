package topicwriter

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hello-pionex/mystic-go/tinyutil"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

type PartitionConfig struct {
	MaxConns      int                     // 最大连接数
	QueueCapacity int                     // 缓冲队列长度
	WaitInternal  time.Duration           // 等待间隔
	MaxWait       func(int) time.Duration // 最大等待时间长度
	MinBytes      int                     // 小于此数据量会触发等待
	MaxBytes      int                     // 批量数据不会超过这个数据量(仅Message.Value)
}

type Partition struct {
	brokers   []string
	topic     string
	partition int

	ch chan *WriteEvent

	pool  *WriterPool
	cfg   *PartitionConfig
	codec kafka.CompressionCodec
}

func (p *Partition) writeEvent(batch *WriteEvent) {
	p.ch <- batch
}

func (p *Partition) loop() {
	cfg := p.cfg

	weight := semaphore.NewWeighted(int64(p.cfg.MaxConns))

	var useConns tinyutil.Int64
	var pendingMessages tinyutil.Int64
	var pendingEvents tinyutil.Int64
	var wroteEvents tinyutil.Int64
	var wroteMessages tinyutil.Int64
	var wroteBytes tinyutil.Int64

	var batch *WriteEvent
	writeEventsPool := NewWriteEventsPool(10000)
	messagesPool := NewMessageSlicePool(10000)
	wg := sync.WaitGroup{}

	log := log.WithFields(logrus.Fields{
		"partition": p.partition,
		"topic":     p.topic,
	})

	go func() {
		for {
			time.Sleep(time.Second * 10)
			wroteMessageNum := wroteMessages.Swap(0)
			wroteEventNum := wroteEvents.Swap(0)
			wroteBytesNum := wroteBytes.Swap(0)
			pendingMsgNum := pendingMessages.Load()
			useConnNum := useConns.Load()
			pendingEventNum := pendingEvents.Load()
			pendingInChannelNum := len(p.ch)

			if wroteMessageNum == 0 &&
				wroteEventNum == 0 &&
				wroteBytesNum == 0 &&
				pendingMsgNum == 0 &&
				useConnNum == 0 &&
				pendingEventNum == 0 &&
				pendingInChannelNum == 0 {
				continue
			}

			log.WithFields(logrus.Fields{
				"pendingMessages":  pendingMsgNum,
				"useConns":         useConnNum,
				"pendingEvents":    pendingEventNum,
				"wroteMessages":    wroteMessageNum,
				"wroteEvents":      wroteEventNum,
				"wroteBytes":       wroteBytesNum,
				"pendingInChannel": pendingInChannelNum,
			}).Infoln("DebugEventPartitionStat")
		}
	}()

	for {
		containSequential := false
		batchs := writeEventsPool.Get()
		events := messagesPool.Get()

		totalByte := 0
		if batch == nil {
			batch = <-p.ch
		}

		batchs = append(batchs, batch)

		// 按消息上限来限定单次的数据数量
		events = append(events, batch.Events...)
		totalByte += batch.TotalBytes
		containSequential = containSequential || batch.Sequential
		batch = nil

		// 根据下游写入的连接数量来选择批量等待的时间
		maxWait := cfg.MaxWait(int(useConns.Load()))

		consumeAndCheck := func() bool {
			for {
				select {
				case batch = <-p.ch:
					// 如果消息累加的大小已经大于限定，则留待下次
					if batch.TotalBytes+totalByte > cfg.MaxBytes {
						return true
					}

					totalByte += batch.TotalBytes
					containSequential = containSequential || batch.Sequential
					events = append(events, batch.Events...)
					batchs = append(batchs, batch)
					batch = nil
				default:
					return totalByte > cfg.MinBytes
				}
			}
		}

		WaitFor(consumeAndCheck, maxWait, time.Millisecond)

		writeFn := func() {
			uuidStr := uuid.New().String()
			conn := p.pool.Get()
			eventLen := len(events)
			batchLen := len(batchs)

			defer func() {
				weight.Release(1)                     // 释放连接
				pendingMessages.Add(-int64(eventLen)) // 堆积消息计数
				wg.Done()                             // 标记完成
				useConns.Add(-1)                      // 并行数量释放
				p.pool.Put(conn)                      // 连接释放
				writeEventsPool.Put(batchs)           // 对象池释放
				messagesPool.Put(events)              // 对象池释放
				wroteMessages.Add(int64(eventLen))    // 统计写入消息数量
				wroteEvents.Add(int64(batchLen))      // 统计处理事件数量
				wroteBytes.Add(int64(totalByte))
			}()

			offset := conn.writeMust(p.codec, events, uuidStr)

			// 响应
			for _, b := range batchs {
				b.Offset = offset // 回报offset
				b.KafkaBatchId = uuidStr
				offset += int64(len(b.Events))
				if b.Wg != nil {
					b.Wg.Done()
				}
			}
		}

		// 要求进入串行模式
		if containSequential {
			wg.Wait() // 等待之前的结束
		}

		// 并行限制
		_ = weight.Acquire(context.Background(), 1)

		// 协程数量加1
		useConns.Add(1)

		wg.Add(1)
		pendingMessages.Add(int64(len(events)))
		go writeFn()

		if containSequential {
			wg.Wait()
		}
	}
}
