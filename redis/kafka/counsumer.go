package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type Counsumer struct {
	ready chan bool
}

func NewCounsumer() *Counsumer {
	c := &Counsumer{
		ready: make(chan bool, 1),
	}
	return c
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Counsumer) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("启动 consumetr 客户端")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Counsumer) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("清理 consumetr 客户端")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Counsumer) ConsumeClaim(session sarama.ConsumerGroupSession, cliaim sarama.ConsumerGroupClaim) error {
	fmt.Println("开始接受kafka 发来的信息。。。")
	for message := range cliaim.Messages() {
		fmt.Println(message)
		// c.messageChan <- message // 将消息放入到一个通道中
		session.MarkMessage(message, "")
	}
	return nil
}
