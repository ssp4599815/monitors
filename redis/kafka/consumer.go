package kafka

import (
	"time"
)

type Consumer struct {
	Channel       chan struct{}
	spool         []struct{}
	nextFlushTime time.Time
	messageChan   chan []struct{}
}

func NewConsumer() *Consumer {
	c := &Consumer{}

	return c
}

func (c *Consumer) Init() {

}

func (c *Consumer) Run() {

	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case message := <-c.Channel:
			c.spool = append(c.spool, message)

			if len(c.spool) == cap(c.spool) {
				c.flush()
			}
		case <-ticker.C:
			if time.Now().After(c.nextFlushTime) {
				c.flush()
			}
		}
	}

}

func (c *Consumer) flush() {

	if len(c.spool) > 0 {
		tmpCopy := make([]struct{}, len(c.spool))
		copy(tmpCopy, c.spool)

		// clear buffer
		c.spool = c.spool[:0]

		// 发送数据给 message 通道
		c.messageChan <- tmpCopy
	}
}
