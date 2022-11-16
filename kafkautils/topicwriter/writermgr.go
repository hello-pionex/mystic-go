package topicwriter

import (
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// 模块功能：持久化和读取事件流

var (
	log = logrus.WithField("pkg", "eventsourcing")
)

type Config struct {
	Brokers    []string
	Topic      string
	Partitions int

	Config *PartitionConfig

	Codec kafka.CompressionCodec
}

type WriteEvent struct {
	Events       []kafka.Message
	Wg           *sync.WaitGroup
	TotalBytes   int
	Offset       int64
	Sequential   bool
	KafkaBatchId string
}

type WriterMgr struct {
	brokers       []string
	topic         string
	partitionList map[int]*Partition
	partitions    int64
	codec         kafka.CompressionCodec
}

func NewWriterMgr(cfg *Config) *WriterMgr {
	mgr := &WriterMgr{
		brokers:       cfg.Brokers,
		topic:         cfg.Topic,
		partitionList: make(map[int]*Partition),
		partitions:    int64(cfg.Partitions),
		codec:         cfg.Codec,
	}

	for i := 0; i < cfg.Partitions; i++ {
		mgr.partitionList[i] = &Partition{
			brokers:   cfg.Brokers,
			topic:     cfg.Topic,
			partition: i,
			ch:        make(chan *WriteEvent, cfg.Config.QueueCapacity),
			pool:      NewWritePool(cfg.Brokers, cfg.Topic, int64(i)),
			cfg:       cfg.Config,
			codec:     cfg.Codec,
		}

		go mgr.partitionList[i].loop()
	}

	return mgr
}

func (writerMgr *WriterMgr) WriteMustByPartition(partition int, batch *WriteEvent) {
	writerMgr.partitionList[partition].writeEvent(batch)
}

func (writerMgr *WriterMgr) TestConnect() error {
	for _, writer := range writerMgr.partitionList {
		conn := writer.pool.Get()
		if err := conn.connect(); err != nil {
			log.WithError(err).Errorln("writer.connect")
			return err
		}
		writer.pool.Put(conn)
	}

	return nil
}
