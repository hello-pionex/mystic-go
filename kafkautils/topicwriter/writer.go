package topicwriter

import (
	"time"

	"github.com/hello-pionex/mystic-go/kafkautils"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Writer struct {
	brokers   []string
	topic     string
	partition int

	conn *kafka.Conn

	statusUpdate time.Time
	status       string

	lastWriteTime time.Time
}

func (writer *Writer) setStatus(status string) {
	writer.statusUpdate = time.Now()
	writer.status = status
}

func (writer *Writer) close() error {
	if writer.conn != nil {
		writer.conn.Close()
	}

	writer.conn = nil

	return nil
}

func (writer *Writer) connect() error {
	if writer.conn != nil {
		writer.conn.Close()
		writer.conn = nil
	}

	conn, err := kafkautils.DialLeader(writer.brokers, writer.topic, writer.partition)
	if err != nil {
		return err
	}

	writer.conn = conn

	return nil
}

func (writer *Writer) connectMust() {
	if writer.conn != nil {
		writer.conn.Close()
	}

	var retry int
	writer.setStatus("BROKEN")
	for {
		seconds := retry
		if seconds > 5 {
			seconds = 5
		}

		// 睡眠
		time.Sleep(time.Duration(seconds) * time.Second)
		retry++
		// 尝试连接msgs
		if err := writer.connect(); err != nil {
			continue
		}
		writer.setStatus("OK")
		break
	}
}

var (
	VerboseWriteKafka = false
)

func (writer *Writer) writeMust(codec kafka.CompressionCodec, msgs []kafka.Message, uuidStr string) int64 {
	reconnect := false
	if writer.conn == nil {
		writer.connectMust()
	}
	var (
		resultOffset = int64(-1)
	)

	tryCount := 0
	since := time.Now()
	defer func() {
		takeTime := time.Since(since)
		if VerboseWriteKafka || takeTime > time.Millisecond*100 {
			log.Infof("writeKafkaMust|%s|%d|%v|%d|%d|%v|%s|",
				writer.topic, writer.partition, time.Since(since), len(msgs), tryCount, reconnect, uuidStr)
		}
	}()

	for {
		tryCount++
		_ = writer.conn.SetDeadline(time.Now().Add(time.Second * 10))
		_, _, offset, _, err := writer.conn.WriteCompressedMessagesAt(codec, msgs...)
		if err != nil {
			totalBytes := 0
			for _, msg := range msgs {
				totalBytes += len(msg.Value)
			}

			log.WithError(err).WithFields(logrus.Fields{
				"topic":      writer.topic,
				"partition":  writer.partition,
				"msgs":       len(msgs),
				"totalBytes": totalBytes,
				"offset":     offset,
				"count":      tryCount,
			}).Errorln("writer.conn.WriteCompressedMessagesAt")

			// 重连并且重试
			writer.connectMust()
			continue
		}

		writer.lastWriteTime = time.Now()

		resultOffset = offset
		break
	}

	return resultOffset
}
