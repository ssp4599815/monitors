package monitor

type Processer struct {
	AlertChan   chan struct{}
	messageChan chan []struct{}
}

func (p *Processer) fetchMessage() {

	for message := range p.messageChan {
		msg = p.parseMessage(message)
		p.analyseMessage(msg)
	}
}

func (p *Processer) parseMessage(msg chan struct{}) string {
	return ""
}

func (p *Processer) analyseMessage(msg string) {

}
