package config

import (
	"time"
)

type Config struct {
	Monitor MonitorConfig
	Redis   []RedisHost
	Kafka   KafkaConfig
	Email   EmailConfig
}

// 监控相关配置

type MonitorConfig struct {
	ConfigPath string `yaml:"config_path"`
}

// redis 相关配置
type RedisHost struct {
	Line     string   `yaml:"line"`
	Password string   `yaml:"password"`
	Addr     []string `yaml:"addr"`
}

// kafka 相关配置
type KafkaConfig struct {
	Version  string   `yaml:"version"`
	Brokers  []string `yaml:"brokers"`
	Consumer `yaml:"consumer"`
}

type Consumer struct {
	Assignor                     string `yaml:"assignor"`
	OffsetCommitInterval         string `yaml:"offset_commit_interval"`
	OffsetCommitIntervalDuration time.Duration
	OffsetOldest                 bool `yaml:"offset_oldest"`
	ReturnErrors                 bool `yaml:"return_errors"`
}

// email 相关配置
type EmailConfig struct {
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Tos      []string `yaml:"tos"`
}
