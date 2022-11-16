package kafkautils

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func createTopic(brokerList []string, returnPartitions bool, topicsArg kafka.TopicConfig) ([]kafka.Partition, error) {
	log := logrus.WithField("topicName", topicsArg.Topic)

	if len(brokerList) == 0 {
		return nil, fmt.Errorf("empty brokers")
	}
	brokerSelected := brokerList[rand.Intn(len(brokerList))]
	conn, err := kafka.Dial("tcp", brokerSelected)
	if err != nil {
		log.WithError(err).Errorln("kafka.Dial brokerSelected")
		return nil, fmt.Errorf("connect broker(%v) failed:%v", brokerSelected, err)
	}

	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		log.WithError(err).Errorln("conn.Controller")
		return nil, fmt.Errorf("find controller broker failed:%v", err)
	}

	var controllerConn *kafka.Conn
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err = kafka.Dial("tcp", controllerAddr)
	if err != nil {
		log.WithError(err).Errorln("Dial controllerAddr")
		return nil, fmt.Errorf("connect controller(%v) failed:%v", controllerAddr, err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(topicsArg)
	if err != nil {
		log.WithError(err).Errorln("controllerConn.CreateTopics")
		return nil, err
	}

	var returnPartitionList []kafka.Partition
	if returnPartitions {
		returnPartitionList, err = controllerConn.ReadPartitions(topicsArg.Topic)
		if err != nil {
			return nil, err
		}
	}

	log = log.WithFields(logrus.Fields{
		"brokers":    brokerList,
		"partitions": topicsArg.NumPartitions,
		"replica":    topicsArg.ReplicationFactor,
	})

	for _, value := range topicsArg.ConfigEntries {
		log = log.WithFields(logrus.Fields{
			value.ConfigName: value.ConfigValue,
		})
	}

	log.Infoln("CreateTopic")
	return returnPartitionList, nil
}

type CreateTopicSimpleCfg struct {
	// BootStrapServer地址
	Brokers []string
	// 主题名字
	TopicName string
	// 复制集合数量，影响数据可靠性和可用性
	// 可靠性：过小会导致故障节点的数据丢失，且没有其他地方恢复
	// 可用性：如果故障节点迟迟不恢复，没有更多其他备份节点用于选主恢复使用
	// 建议3
	ReplicationFactor int
	// 分片数量
	Partitions int
	// 保留时间
	RetentionTime *time.Duration
	// 保留大小
	RetentionBytes *int64
	// 允许ISR中的brokers都出现故障时，还在此之外Brokers进行选主（此情况下会导致数据丢失），建议false
	UncleanLeaderElectionEnable *bool
	// 写入时，需同步确认成功的份数 （过小的数量会导致成功节点故障时数据丢失），建议2
	MinInSyncReplicas *int
	// 创建之后返回服务器端的Partitions列表
	ReturnPartitions bool
	// 使用服务器时间
	UseLogAppendTime bool
}

// CreateTopicWithSimpleCfg 最常用的创建
func CreateTopicWithSimpleCfg(cfg *CreateTopicSimpleCfg) ([]kafka.Partition, error) {
	entries := make([]kafka.ConfigEntry, 0, 3)

	if cfg.MinInSyncReplicas != nil {
		entries = append(entries, kafka.ConfigEntry{
			ConfigName:  "min.insync.replicas",
			ConfigValue: fmt.Sprintf("%d", *cfg.MinInSyncReplicas),
		})
	}

	if cfg.UncleanLeaderElectionEnable != nil {
		entries = append(entries, kafka.ConfigEntry{
			ConfigName:  "unclean.leader.election.enable",
			ConfigValue: fmt.Sprintf("%v", *cfg.UncleanLeaderElectionEnable),
		})
	}

	if cfg.RetentionTime != nil {
		entries = append(entries, kafka.ConfigEntry{
			ConfigName:  "retention.ms",
			ConfigValue: fmt.Sprintf("%d", int64(*cfg.RetentionTime)/1e6), // 保存时间
		})
	}

	if cfg.RetentionBytes != nil {
		entries = append(entries, kafka.ConfigEntry{
			ConfigName:  "retention.bytes",
			ConfigValue: fmt.Sprintf("%d", cfg.RetentionBytes), // 保存时间
		})
	}

	if cfg.UseLogAppendTime {
		entries = append(entries, kafka.ConfigEntry{
			ConfigName:  "message.timestamp.type",
			ConfigValue: "LogAppendTime", // 保存时间
		})
	}

	return createTopic(cfg.Brokers, cfg.ReturnPartitions, kafka.TopicConfig{
		Topic:             cfg.TopicName,
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ConfigEntries:     entries,
	})
}
