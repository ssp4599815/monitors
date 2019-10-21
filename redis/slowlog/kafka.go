package slowlog

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	wg      sync.WaitGroup
	brokers = []string{"10.211.55.13:9092"}
	version = "2.1.1"
	groupID = "redis-slowlog-consumer"
	topic   = []string{"redis-slowlog"}
)



func Config() {
	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	// 设置 consumer 的配置
	config := sarama.NewConfig()
	// 设置版本号
	config.Version = version
	// 提交offset的间隔时间，每秒提交一次给kafka
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	// 初始从最新的offset开始
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	// 设置 消费组 reblance时的模式
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	// 如果发生错误，就将错误返回给 Errors channel ，默认为 false
	config.Consumer.Return.Errors = true
}

func NewConsumerGroup() {

}

func Init() {

}

func Run() {

}

func main() {
	fmt.Println("Start a new Sarama consumer")

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	// 设置 consumer 的配置
	config := sarama.NewConfig()
	// 设置版本号
	config.Version = version
	// 提交offset的间隔时间，每秒提交一次给kafka
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	// 初始从最新的offset开始
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	// 设置 消费组 reblance时的模式
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	// 如果发生错误，就将错误返回给 Errors channel ，默认为 false
	config.Consumer.Return.Errors = true

	// 创建一个新的 消费者组
	consumer := Counsumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, groupID, config)

	if err != nil {
		log.Panicf("Error creating consumer group client:%v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, topic, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled,signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminatiing: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Consumer 代表 一个 Sarama 消费者组来消费
type Counsumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Counsumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Counsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Counsumer) ConsumeClaim(session sarama.ConsumerGroupSession, cliaim sarama.ConsumerGroupClaim) error {
	for message := range cliaim.Messages() {
		log.Println(message)
		session.MarkMessage(message, "")
	}
	return nil
}
