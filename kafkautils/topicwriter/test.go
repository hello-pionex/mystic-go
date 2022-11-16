package topicwriter

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

func NewTestWriteEvent(sequential bool, wg *sync.WaitGroup) *WriteEvent {
	return &WriteEvent{
		Events:     []kafka.Message{{Value: []byte("test")}},
		TotalBytes: 300,
		Sequential: false,
	}
}
