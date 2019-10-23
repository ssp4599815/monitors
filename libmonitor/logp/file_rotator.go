package logp

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	RotatorMaxFiles         = 1024             // 最多保存1024个文件
	DefaultKeepFiles        = 7                // 默认保存7个文件
	DefaultRotateEveryBytes = 10 * 1024 * 1024 // 默认最大保存 10M 的文件
)

type FileRotator struct {
	Path             string // 要切割的文件的目录
	Name             string // 要切割的文件的大小
	RotateEveryBytes *uint64
	KeepFiles        *int

	current     *os.File // 当前正在切割的文件
	currentSize uint64   // 当前正在切割文件的大小
}

// 创建日志文件的目录
func (rotator *FileRotator) CreateDirectory() error {
	fileinfo, err := os.Stat(rotator.Path)
	if err == nil {
		// 要对一个目录下的文件进行切割
		if !fileinfo.IsDir() { // 如果不是一个目录
			return fmt.Errorf("%s exists but it's not a directory", rotate.Path)
		}
	}

	if os.IsNotExist(err) { // 如果文件不存在就创建
		err = os.MkdirAll(rotator.Path, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

// 获取一个新的 fileNo 结尾的文件
func (rotator *FileRotator) FilePath(fileNo int) string {
	if fileNo == 0 {
		return filepath.Join(rotator.Path, rotator.Name)
	}
	filename := strings.Join([]string{rotator.Name, strconv.Itoa(fileNo)}, ".")
	return filepath.Join(rotator.Path, filename)
}

// FileExists 判断一个文件是否存在
func (rotator *FileRotator) FileExists(fileNo int) bool {
	filePath := rotator.FilePath(fileNo) // 获取当前要切割文件的路径
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}

	return true
}

func (rotator *FileRotator) CheckIfConfigSane() error {
	if len(rotator.Name) == 0 {
		return fmt.Errorf("File logging require a name for the file names\n")
	}

	if rotator.KeepFiles == nil {
		rotator.KeepFiles = new(int)
		*rotator.KeepFiles = DefaultKeepFiles
	}

	if rotator.RotateEveryBytes == nil {
		rotator.RotateEveryBytes = new(uint64)
		*rotator.RotateEveryBytes = DefaultRotateEveryBytes
	}

	if *rotator.KeepFiles < 2 || *rotator.KeepFiles > RotatorMaxFiles {
		return fmt.Errorf("The number of files to keep should be between 2 and %d\n", RotatorMaxFiles-1)
	}

	return nil
}

// 将 日志文件写入到 文件中，每次写入都进行一次判断，是否需要切割
func (rotator *FileRotator) WriteLine(line []byte) error {
	if rotator.shouldRotate() {
		err := rotator.Rotate()
		if err != nil {
			return err
		}
	}
	_, err := rotator.current.Write(line) // 将日志写入到文件中
	if err != nil {
		return err
	}
	_, err = rotator.current.Write([]byte("\n")) // 最后别忘了加上 换行符
	if err != nil {
		return err
	}

	rotator.currentSize += uint64(len(line) + 1) // 统计每一行的大小

	return nil
}

// 判断一个文件是否需要进行切割
func (rotator *FileRotator) shouldRotate() bool {
	if rotator.current == nil {
		return true
	}

	if rotator.currentSize >= *rotator.RotateEveryBytes {
		return true
	}
	return false
}

func (rotator *FileRotator) Rotate() error {

	// 打开当前文件有误，然后关闭文件描述符
	if rotator.current != nil {
		if err := rotator.current.Close(); err != nil {
			return err
		}
	}

	// delete any extra files, normally wo shouldn't have any
	for fileNo := *rotator.KeepFiles; fileNo < RotatorMaxFiles; fileNo++ {
		if rotator.FileExists(fileNo) {
			err := os.Remove(rotator.FilePath(fileNo))
			if err != nil {
				return err
			}
		}
	}

	// shift all files from last to first
	for fileNo := *rotator.KeepFiles; fileNo < RotatorMaxFiles; fileNo-- {
		if rotator.FileExists(fileNo) {
			// 文件不存在，不做任何操作
			continue
		}

		filePath := rotator.FilePath(fileNo)
		if rotator.FileExists(fileNo + 1) {
			// next file exists, something is strange
			return fmt.Errorf("File %s exists, when rotating would overwrite it\n", rotator.FilePath(fileNo+1))
		}

		// 每次切割，都会将 轮询的 当前的文件后缀+1
		err := os.Rename(filePath, rotator.FilePath(fileNo+1))
		if err != nil {
			return err
		}
	}

	// 每次切割完后，需要创建一个新的文件
	filePath := rotator.FilePath(0)
	current, err := os.Create(filePath)
	if err != nil {
		return err
	}
	rotator.current = current // 将新创建的文件描述符赋值给 rotate.current 对象
	rotator.currentSize = 0

	// 移除超过个数限制的文件，并忽略错误
	filePath = rotator.FilePath(*rotator.KeepFiles)
	_ = os.Remove(filePath)

	return nil
}
