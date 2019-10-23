package monitor

import (
	"fmt"
	"github.com/ssp4599815/monitors/libmonitor/logp"
	"log"
)

// 定义一个基础的 monitor 接口，所有的监控系统都要实现这些方法
type Moniter interface {
	Config(*Monitor) error
	Setup(*Monitor) error
	Run(*Monitor) error
	Cleanup(*Monitor) error
	Stop()
}

// 定义一个基础的monitor 对象
type Monitor struct {
	Name    string
	Version string
	Config  *MonitorConfig
	MT      Moniter
}

type MonitorConfig struct {
	Logging *logp.Logging
	// email
}

func NewMonitor(name string, version string, mt Moniter) *Monitor {
	m := Monitor{
		Name:    name,
		Version: version,
		MT:      mt,
	}
	return &m
}

func (m *Monitor) LoadConfig() {

	// 初始化日志记录
	err := logp.Init(m.Name, m.Config.Logging)
	if err != nil {
		fmt.Printf("Error initializing logging: %v\n", err)
	}
}

func (m *Monitor) Run() {
	// 启动前的准备工作
	err := m.MT.Setup(m)
	if err != nil {
		log.Fatalf("Setup returned an error: %v", err)
	}

	// 处理退出的信号
	// service.HandleSignals(m.MT.Stop)

	log.Printf("%s successfully setup, Start running.", m.Name)

	// 正式启动监控程序
	err = m.MT.Run(m)
	if err != nil {
		log.Panicf("Run returned an error:%v", err)
	}

	// log.Printf("Cleaning up %s before shutting down.", m.Name)

	// 清理工作
	err = m.MT.Cleanup(m)
	if err != nil {
		log.Printf("Cleanup returned an error: %v", err)
	}
}
