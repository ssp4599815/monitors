package logp

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type Logging struct {
	Files *FileRotator
	Level string
}

var rotator FileRotator

// 初始化日志系统  name 为日志的目录名
func Init(name string, config *Logging) error {
	// 设置日志格式为 json格式
	log.SetFormatter(&log.JSONFormatter{})

	// 设置日志输出位置
	log.SetOutput(os.Stdout)

	// 解析日志级别
	logLevel, err := log.ParseLevel(config.Level)
	if err != nil {
		return err
	}
	// 设置日志级别
	log.SetLevel(logLevel)

	defaultFilePath := fmt.Sprintf("/var/log/%s", name) // 设置日志的默认路径

	if config.Files == nil {
		config.Files = &FileRotator{
			Path: defaultFilePath,
			Name: name,
		}
	} else {
		if config.Files.Path == "" {
			config.Files.Path = defaultFilePath
		}

		if config.Files.Name == "" {
			config.Files.Name = name
		}
	}

	err = SetToFile(config.Files)
	if err != nil {
		return err
	}

	return nil
}

func SetToFile(rotator *FileRotator) error {
	err := rotator.CreateDirectory()
	if err != nil {
		return err
	}

	err = rotator.CheckIfConfigSane()
	if err != nil {
		return err
	}

	return nil
}

func send(prefix string, format string, v ...interface{}) {
	prefix = time.Now().Format(time.RFC3339) + " " + prefix
	_ = rotator.WriteLine([]byte(fmt.Sprintf(prefix+format, v...)))
}
