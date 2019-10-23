package main

import (
	"github.com/ssp4599815/monitors/libmonitor/monitor"
	. "github.com/ssp4599815/monitors/redis/monitor"
	"log"
)

var (
	Name    = "redis-monitor"
	Version = "1.0.0"
)

func main() {

	// 初始化 monitor 对象
	rm := RedisMonitor{}

	m := monitor.NewMonitor(Name, Version, &rm)

	// 配置 redis monitor
	err := rm.Config(m)
	if err != nil {
		log.Fatalf("Config error: %v", err)
	}

	// 正式运行监控程序mo
	m.Run()
}
