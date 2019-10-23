package slowlog

import "fmt"

type Processer struct {
	AlertChan   chan struct{}
	messagesChan chan []struct{}
}

func (p *Processer) fetchMessage() {

	for message := range p.messagesChan {
		fmt.Println(message)
		// msg = p.parseMessage(message)
		// p.analyseMessage(msg)
	}
}

//func (p *Processer) parseMessage(msg chan struct{}) string {
//	return ""
//}
//
//func (p *Processer) analyseMessage(msg string) {
//
//}
