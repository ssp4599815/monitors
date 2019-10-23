package slowlog

import (
	"fmt"
	cfg "github.com/ssp4599815/monitors/redis/config"
	"github.com/ssp4599815/monitors/redis/kafka"
)

type SlowLog struct {
	KafkaConfig *cfg.KafkaConfig
	Channel     chan []*msgEvent
}

// 接受来自kafka 的 slowlog 事件
type msgEvent struct {
}

func NewSlowLog(kafkaConfig *cfg.KafkaConfig) *SlowLog {
	s := &SlowLog{
		KafkaConfig: kafkaConfig,
		Channel:     make(chan []*msgEvent, 1),
	}
	return s
}

func (s *SlowLog) Run() {
	fmt.Println("开启 SlowLog Monitor...")
	consumer := kafka.NewConsumerGroupHandler(s.KafkaConfig)
	consumer.Start()
}
