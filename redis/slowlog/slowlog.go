package slowlog

import (
	"fmt"
)

type SlowLog struct {
	Channel chan []*SlowLogEvent
}

// 接受来自kafka 的 slowlog 事件
type SlowLogEvent struct {
}

func NewSlowLog() *SlowLog {
	s := &SlowLog{
		Channel: make(chan []*SlowLogEvent, 1),
	}
	return s
}

func (s *SlowLog) Init() {


}

func (s *SlowLog) Run() {
	fmt.Println("Starting SlowLog Monitor.")

	for {
		select {
		case events := <-s.Channel:
			s.processEvents(events)

		}
	}
}

func (s *SlowLog) processEvents(events []*SlowLogEvent) {
	fmt.Printf(" Processing %d events", len(events))

	for _, event := range events {
		fmt.Print(event)
	}
}
