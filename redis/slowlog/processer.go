package slowlog

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tidwall/gjson"
	"reflect"
	"time"
)

type Processer struct {
	AlertChan    chan struct{}
	messagesChan chan *sarama.ConsumerMessage
	Slowlog      *Slowlog
	Slowlogs     []*Slowlog
	MaxSize      int
}

func NewProcesser(msgChan chan *sarama.ConsumerMessage) *Processer {
	p := &Processer{
		messagesChan: msgChan,
		MaxSize:      100,
	}
	return p
}

func (p *Processer) Run() {

	for message := range p.messagesChan {
		slowlog := p.parseMessage(message)
		p.Slowlogs = append(p.Slowlogs, slowlog)
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

	//p.Slowlog.Hostname = gjson.GetBytes(messageValue, "host.name").String()
	//p.Slowlog.Redis.ID = gjson.GetBytes(messageValue, "redis.slowlog.id").Int()
	//p.Slowlog.Redis.Cmd = gjson.GetBytes(messageValue, "redis.slowlog.cmd").String()
	//p.Slowlog.Redis.Key = gjson.GetBytes(messageValue, "redis.slowlog.key").String()
	//p.Slowlog.Redis.Duration = gjson.GetBytes(messageValue, "redis.slowlog.duration.us").Int()
	//for _, arg := range gjson.GetBytes(messageValue, "redis.slowlog.args").Array() {
	//	p.Slowlog.Redis.Args = append(p.Slowlog.Redis.Args, arg.String())
	//}
	fmt.Printf("%#v", p.Slowlog)
	return nil
}

func (p *Processer) analyseMessage(msg string) {

}
