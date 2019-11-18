package rdb

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"testing"
)

func TestRDB(t *testing.T) {
	baseDir, _ := os.Getwd()
	rdbFile := path.Join(baseDir, "dumps", "dump.rdb")
	fmt.Println(rdbFile)
	handler, err := os.Open(rdbFile)
	if err != nil {
		fmt.Println("read rdb file err ,", err)
	}
	r := NewRDB(bufio.NewReader(handler))
	_ = r.Parse()
}
