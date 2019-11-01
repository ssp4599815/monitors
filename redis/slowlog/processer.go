package slowlog

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tidwall/gjson"
	"reflect"
	"time"
)

type Processer struct {
	AlertChan           chan struct{}
	messageChan         chan *sarama.ConsumerMessage
	Slowlog             *Slowlog
	Slowlogs            []*Slowlog
	MaxSize             int
	IdleTimeoutDuration time.Duration // 刷新的空闲时间
}

func NewProcesser(msgChan chan *sarama.ConsumerMessage) *Processer {
	p := &Processer{
		messageChan: msgChan,
		MaxSize:     100, // 达到100条就报警
		Slowlog:     new(Slowlog),
		Slowlogs:    make([]*Slowlog, 0),
	}
	return p
}

func (p *Processer) Run() {
	fmt.Println("开始处理 messagesChan 通道中的数据")

	ticker := time.NewTicker(p.IdleTimeoutDuration)

	select {
	case message := <-p.messageChan:
		slowlog := p.parseMessage(message)
		p.Slowlogs = append(p.Slowlogs, slowlog)
		if len(p.Slowlogs) == p.MaxSize {
			p.analyseMessage(p.Slowlogs)
		}
	case <-ticker.C:
		p.analyseMessage(p.Slowlogs)
	}
}

func (p *Processer) parseMessage(msg *sarama.ConsumerMessage) *Slowlog {
	messageValue := msg.Value

	fmt.Println(string(messageValue))

	formatTimeStr := gjson.GetBytes(messageValue, "@timestamp").String()
	formatTime, err := time.Parse(time.RFC3339Nano, formatTimeStr)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(formatTime)
	fmt.Println(reflect.TypeOf(formatTime))
	p.Slowlog.Timestamp = formatTime

	p.Slowlog.Hostname = gjson.GetBytes(messageValue, "host.name").String()
	p.Slowlog.Redis.ID = gjson.GetBytes(messageValue, "redis.slowlog.id").Int()
	p.Slowlog.Redis.Cmd = gjson.GetBytes(messageValue, "redis.slowlog.cmd").String()
	p.Slowlog.Redis.Key = gjson.GetBytes(messageValue, "redis.slowlog.key").String()
	p.Slowlog.Redis.Duration = gjson.GetBytes(messageValue, "redis.slowlog.duration.us").Int()
	for _, arg := range gjson.GetBytes(messageValue, "redis.slowlog.args").Array() {
		p.Slowlog.Redis.Args = append(p.Slowlog.Redis.Args, arg.String())
	}
	fmt.Printf("%#v", p.Slowlog)
	return p.Slowlog
}

func (p *Processer) analyseMessage(msgs []*Slowlog) {
	// 如果没有数据 就天国
	if len(msgs) > 0 {
		// TODO：处理数据
	}

}
