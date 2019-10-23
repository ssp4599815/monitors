package service

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
)

func HandleSignals(stopFunction func()) {
	var callback sync.Once

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Kill, os.Interrupt) // 监听 kill 和 Ctrl+c 的信号
	go func() {
		<-sigc
		fmt.Println("Recevied sigterm/sigint, stopping")
		callback.Do(stopFunction)
	}()
}
