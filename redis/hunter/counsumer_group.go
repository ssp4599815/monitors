package hunter

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	cfg "github.com/ssp4599815/monitors/redis/config"
	"log"
	"sync"
	"time"
)

type ConsumerGroupHandler struct {
	wg            sync.WaitGroup // 用于阻塞 consumer goroutiune
	consumerGroup sarama.ConsumerGroup
	saramaConfig  *sarama.Config
	consumer      *Counsumer
	kafkaConfig   cfg.KafkaConfig
	MessageChan   chan *sarama.ConsumerMessage // 从 kafka 接受信息, 传给下层
}

// 接受来自上层的 KafkaConfig 配置文件信息
func NewConsumerGroupHandler(kafkaConfig cfg.KafkaConfig, MessageChan chan *sarama.ConsumerMessage) *ConsumerGroupHandler {

	cgh := &ConsumerGroupHandler{
		saramaConfig: sarama.NewConfig(),
		kafkaConfig:  kafkaConfig,
		MessageChan:  MessageChan,
	}

	fmt.Println("初始化 kafka consumer group 配置文件")
	cgh.initConfig()
	fmt.Println("初始化 consumer group 对象")
	cgh.initConsumerGroup()

	return cgh
}
func (c *ConsumerGroupHandler) parseKafkaVersion() sarama.KafkaVersion {
	version, err := sarama.ParseKafkaVersion(c.kafkaConfig.Version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	return version
}

// 初始化相关 sarama 配置
func (c *ConsumerGroupHandler) initConfig() {
	c.saramaConfig.Version = c.parseKafkaVersion()

	// 提交offset的间隔时间，每秒提交一次给kafka
	c.saramaConfig.Consumer.Offsets.CommitInterval = 1 * time.Second

	if c.kafkaConfig.OffsetOldest {
		// 初始从最新的offset开始
		c.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// 如果发生错误，就将错误返回给 Errors channel ，默认为 false
	if c.kafkaConfig.ReturnErrors {
		c.saramaConfig.Consumer.Return.Errors = c.kafkaConfig.ReturnErrors
	}

	// 设置 消费组 reblance时的模式
	switch c.kafkaConfig.Assignor {
	case "sticky":
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		// 默认设置为 sticky
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
		log.Panicf("Unrecognized consumer group partition assignor: %s", c.kafkaConfig.Assignor)
	}
}

func (c *ConsumerGroupHandler) initConsumerGroup() {
	var err error

	// 创建一个新的 consumer 对象
	c.consumer = NewCounsumer(c.MessageChan)

	// 创建一个 consumergroup 对象
	fmt.Println("开始创建 ConsumerGroup 对象")
	c.consumerGroup, err = sarama.NewConsumerGroup(c.kafkaConfig.Brokers, c.kafkaConfig.GroupID, c.saramaConfig)
	if err != nil {
		log.Println(err)
	}
}

func (c *ConsumerGroupHandler) Start() {
	fmt.Println("启动一个新的 Sarama consumer")
	go c.handlerError()
	c.wg.Add(1)
	go c.handlerMessage()
	c.wg.Wait()
}

// 开始处理错误
func (c *ConsumerGroupHandler) handlerError() {
	fmt.Println("开始处理Sarama consumer 错误")
	for err := range c.consumerGroup.Errors() {
		fmt.Printf("sarama: %s", err)
	}
}

// 开始处理监控到的数据
func (c *ConsumerGroupHandler) handlerMessage() {
	fmt.Println("开始处理Sarama consumer Message")
	defer c.wg.Done()
	for {
		err := c.consumerGroup.Consume(context.Background(), c.kafkaConfig.Topic, c.consumer)
		if err != nil {
			log.Panicln("Error from consumer: ", err)
		}
	}
}

func (c *ConsumerGroupHandler) Stop() {
	log.Println("Initiating shutdown of consumer group...")
	err := c.consumerGroup.Close()
	if err != nil {
		fmt.Println("Error closing client:", err)
	}
}
