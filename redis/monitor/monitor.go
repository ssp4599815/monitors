package monitor

import (
	"fmt"
	"github.com/ssp4599815/monitors/libmonitor/cfgfile"
	"github.com/ssp4599815/monitors/libmonitor/monitor"
	cfg "github.com/ssp4599815/monitors/redis/config"
	. "github.com/ssp4599815/monitors/redis/slowlog"
	"log"
)

type RedisMonitor struct {
	RDSConfig *cfg.Config
	SlowLog   *SlowLog
}

func (rm *RedisMonitor) Config(m *monitor.Monitor) error {
	// 加载配置文件
	err := cfgfile.Read(&rm.RDSConfig, "")

	// fmt.Printf("%#v\n", rm.RDSConfig)

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

	// 启动 slowlog 监听程序
	rm.SlowLog = NewSlowLog(&rm.RDSConfig.Kafka)
	rm.SlowLog.Run()

	return nil
}

func (rm *RedisMonitor) Cleanup(m *monitor.Monitor) error {
	return nil
}

func (rm *RedisMonitor) Stop() {

}
