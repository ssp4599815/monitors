package hunter

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	cfg "github.com/ssp4599815/monitors/redis/config"
	"github.com/ssp4599815/monitors/redis/monitor"
	"log"
	"sync"
	"time"
)

type ConsumerGroupHandler struct {
	wg sync.WaitGroup // 用于阻塞 consumer goroutiune

	RedisMonitor *monitor.RedisMonitor // 接受前面传过来的配置文件

	consumerGroup sarama.ConsumerGroup
	consumer      *Counsumer
	saramaConfig  *sarama.Config
	kafkaConfig   *cfg.KafkaConfig

	messageChan   chan *sarama.ConsumerMessage   // 从 kafka 接受信息，需要定义长度
	messagesChan  chan []*sarama.ConsumerMessage // 用于将消息发C送给 processer
	spool         []*sarama.ConsumerMessage      // 缓冲区
	spoolSize     int                            // 缓冲区大小
	nextFlushTime time.Time                      // 刷新缓冲区的间隔
}

// 接受来自上层的 KafkaConfig 配置文件信息
func NewConsumerGroupHandler(kafkaConfig *cfg.KafkaConfig) *ConsumerGroupHandler {

	cgh := &ConsumerGroupHandler{
		spool:        make([]*sarama.ConsumerMessage, 100),
		spoolSize:    10,
		saramaConfig: sarama.NewConfig(),
		kafkaConfig:  kafkaConfig,
		messageChan:  make(chan *sarama.ConsumerMessage, 1000), // 初始化一个 能接受1000条信息的通道
	}

	cgh.config()
	cgh.Init()

	return cgh
}
func (c *ConsumerGroupHandler) parseKafkaVersion() sarama.KafkaVersion {
	version, err := sarama.ParseKafkaVersion(c.RedisMonitor.RDSConfig.Kafka.Version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	return version
}

// 初始化相关 sarama 配置
func (c *ConsumerGroupHandler) config() {
	c.saramaConfig.Version = c.parseKafkaVersion()

	// 提交offset的间隔时间，每秒提交一次给kafka
	c.saramaConfig.Consumer.Offsets.CommitInterval = 1 * time.Second

	if c.RedisMonitor.RDSConfig.Kafka.OffsetOldest {
		// 初始从最新的offset开始
		c.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// 设置 消费组 reblance时的模式
	switch c.RedisMonitor.RDSConfig.Kafka.Assignor {
	case "sticky":
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		// 默认设置为 sticky
		c.saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
		log.Panicf("Unrecognized consumer group partition assignor: %s", c.RedisMonitor.RDSConfig.Kafka.Assignor)
	}

	// 如果发生错误，就将错误返回给 Errors channel ，默认为 false
	c.saramaConfig.Consumer.Return.Errors = c.RedisMonitor.RDSConfig.Kafka.ReturnErrors
}

func (c *ConsumerGroupHandler) Init() {
	var err error

	// 创建一个新的 consumer 队形
	c.consumer = NewCounsumer(c.messageChan)

	// 创建一个 consumergroup 对象
	c.consumerGroup, err = sarama.NewConsumerGroup(c.kafkaConfig.Brokers, c.kafkaConfig.GroupID, c.saramaConfig)
	if err != nil {
		log.Println(err)
	}
}

func (c *ConsumerGroupHandler) Run() {
	fmt.Println("启动一个新的 Sarama consumer")
	go c.handlerError()
	c.wg.Add(2)
	go c.handlerMessage()
	go c.flushMessage()
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
		fmt.Println("创建： c.consumerGroup.Consume ")
		err := c.consumerGroup.Consume(context.Background(), c.KafkaConfig.Topic, c.consumer)
		fmt.Println("创建完成： c.consumerGroup.Consume")
		if err != nil {
			log.Panicln("Error from consumer: ", err)
		}
	}
}

func (c *ConsumerGroupHandler) flushMessage() {
	defer c.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case message := <-c.messageChan:
			c.spool = append(c.spool, message) // 将消息累积到一定数量后发送
			if len(c.spool) == cap(c.spool) {
				c.flush()
			}
		case <-ticker.C: // 将消息达到 指定空闲时间 后发送
			if time.Now().After(c.nextFlushTime) {
				c.flush()
			}
		}
	}
}

func (c *ConsumerGroupHandler) flush() {
	if len(c.spool) > 0 {
		tmpCopy := make([]*sarama.ConsumerMessage, len(c.spool))
		copy(tmpCopy, c.spool)

		// clear buffer
		c.spool = c.spool[:0]

		fmt.Println("发消息给 message 通道")
		// 发送数据给 message 通道
		c.messagesChan <- tmpCopy
	}
}

func (c *ConsumerGroupHandler) Stop() {
	log.Println("Initiating shutdown of consumer group...")
	err := c.consumerGroup.Close()
	if err != nil {
		fmt.Println("Error closing client:", err)
	}
}
