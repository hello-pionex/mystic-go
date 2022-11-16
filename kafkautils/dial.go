package kafkautils

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// DialLeader 封装从集群中得到topic/partition的leader连接
func DialLeader(addresses []string, topic string, partition int) (*kafka.Conn, error) {
	log := logrus.WithFields(logrus.Fields{
		"topic":     topic,
		"partition": partition,
	})

	var retErr error
	for _, address := range addresses {
		// 查询分片所在broker，并且创建连接
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
		defer cancelFunc()
		conn, err := kafka.DialLeader(ctx, "tcp", address, topic, partition)
		if err != nil {
			log.WithError(err).Errorln("DialLeader")
			retErr = err
			continue
		}

		log.WithFields(logrus.Fields{
			"localAddr": conn.LocalAddr(),
		}).Info("NewKafkaConnect")
		return conn, nil
	}

	return nil, fmt.Errorf("all brokers failed,%w", retErr)
}
