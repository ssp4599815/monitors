package hunter

import (
	"github.com/Shopify/sarama"
	"github.com/ssp4599815/monitors/redis/config"
	"time"
)

type Hunter struct {
	MessageChan   chan *sarama.ConsumerMessage // 从 kafka 接受信息, 传给下层
	KafkaConfig   config.KafkaConfig          // 传给下层
	nextFlushTime time.Time                    // 刷新缓冲区的间隔
}

func NewHunter(kafkaConfig config.KafkaConfig, msgChan chan *sarama.ConsumerMessage) *Hunter {
	h := &Hunter{
		KafkaConfig: kafkaConfig,
		MessageChan: msgChan, // 初始化一个 能接受1000条信息的通道
	}
	return h
}

func (h *Hunter) Run() {
	handler := NewConsumerGroupHandler(h.KafkaConfig, h.MessageChan)
	go handler.Start()
}
