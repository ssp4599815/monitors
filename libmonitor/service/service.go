package service

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func HandleSignals(stopFunction func()) {
	var callback sync.Once

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Println("Recevied sigterm/sigint, stopping")
		callback.Do(stopFunction)
	}()
}
