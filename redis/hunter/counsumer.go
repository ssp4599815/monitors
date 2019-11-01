package hunter

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type Counsumer struct {
	messageChan chan *sarama.ConsumerMessage
}

func NewCounsumer(msgChan chan *sarama.ConsumerMessage) *Counsumer {
	c := &Counsumer{
		messageChan: msgChan, // 上面在创建consumer的时候传进来的一个通道，用于将接收到的信息传入到这个通道中
	}
	return c
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Counsumer) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("正在启动 consumetr 客户端")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Counsumer) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("开始清理 consumetr 客户端")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Counsumer) ConsumeClaim(session sarama.ConsumerGroupSession, cliaim sarama.ConsumerGroupClaim) error {
	fmt.Println("开始接受kafka 发来的信息。。。")
	for message := range cliaim.Messages() {
		c.messageChan <- message // 将消息放入到一个通道中
		fmt.Println("当前 messageChan 队列的长度：", len(c.messageChan))
		session.MarkMessage(message, "")
	}
	return nil
}
