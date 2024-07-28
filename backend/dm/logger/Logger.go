package logger

import (
	"dbofmine/backend/utils"
	"dbofmine/commons"
	"os"
)

var (
	SEED int32 = 13331
	// OffsetSize 偏移大小
	OffsetSize = 0

	// LogItemLengthSize 用来表示每一个日志项最开始表示后面Data的长度的字节数
	LogItemLengthSize = 4
	// OffsetCheckSumSize 用作日志文件全局校验的偏移
	OffsetCheckSumSize = OffsetSize + LogItemLengthSize
	// CheckSumSize 校验和大小
	CheckSumSize = 4
	// OffsetDataSize 用作日志文件中日志条目开始的部分的偏移
	OffsetDataSize = OffsetCheckSumSize + CheckSumSize
	// LogSuffix 日志文件后缀
	LogSuffix = ".log"
)

type DBLogger struct {
	file *os.File

	lock commons.ReentrantLock

	// 当前日志指针的位置
	currentPosition int64
	// 初始化时记录一下，当进行log操作时不更新此值
	fileSize int64
	// xCheckSum 全局校验和
	xCheckSum int32
}

// CreateLogger 创建一个新的日志管理器
func CreateLogger(path string) *DBLogger {
	// 创建日志文件，不能存在
	if utils.FileExists(path + LogSuffix) {
		panic(commons.ErrorMessage.FileExistError)
	}

	file, err := os.OpenFile(path+LogSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		panic(err)
	}

	// 创建日志文件，写入一开始的校验和（4字节的0）
	_, err = file.Write([]byte{0, 0, 0, 0})
	if err != nil {
		panic(err)
	}
	file.Sync()

	return NewLogger(file)
}

// OpenLogger 打开一个已经存在的日志文件
func OpenLogger(path string) *DBLogger {
	// 日志文件必须存在
	file, err := os.OpenFile(path+LogSuffix, os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}

	logger := NewLogger(file)
	logger.init()

	return logger
}

// NewLogger 创建一个新的日志管理器
func NewLogger(file *os.File, xCheckSum ...int32) *DBLogger {
	if len(xCheckSum) == 0 {
		return &DBLogger{
			file: file,
		}
	}

	return &DBLogger{
		file:      file,
		xCheckSum: xCheckSum[0],
	}
}
