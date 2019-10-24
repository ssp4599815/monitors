package slowlog

import "time"

type Slowlog struct {
	Timestamp time.Time
	Redis     struct {
		ID       int64
		Cmd      string
		Key      string
		Args     []string
		Duration int64
	}
	Hostname string // 主机名
}
