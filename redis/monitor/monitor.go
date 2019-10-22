package monitor

import (
	"fmt"
	"github.com/ssp4599815/monitors/libmonitor/cfgfile"
	"github.com/ssp4599815/monitors/libmonitor/monitor"
	cfg "github.com/ssp4599815/monitors/redis/config"
	"log"
)

type RedisMonitor struct {
	RDSConfig *cfg.Config
}

func (rm *RedisMonitor) Config(m *monitor.Monitor) error {
	// 加载配置文件
	err := cfgfile.Read(&rm.RDSConfig, "")

	fmt.Printf("%#v\n", rm.RDSConfig.Monitor.ConfigPath)

	if err != nil {
		return fmt.Errorf("Error reading config file: %v\n", err)
	}
	return nil
}

func (rm *RedisMonitor) Setup(m *monitor.Monitor) error {
	return nil
}

func (rm *RedisMonitor) Run(m *monitor.Monitor) error {
	// 处理错误
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		log.Fatalf("recovered panic: %v", p)
	}()

	//// 启动 slowlog 监听程序
	//m.SlowLog = NewSlowLog()
	//m.SlowLog.Init()
	//
	//go m.SlowLog.Run()
	return nil
}

func (rm *RedisMonitor) Cleanup(m *monitor.Monitor) error {
	return nil
}

func (rm *RedisMonitor) Stop() {

}
