package logger

import (
	"SimpleDB/backend/utils"
	"SimpleDB/commons"
	"encoding/binary"
	"errors"
)

// init 对logger进行初始化，主要是获取文件大小并进行文件大小和校验和的校验
func (logger *DBLogger) init() {
	// 获取文件大小
	size, err := utils.GetFileSize(logger.file)
	if err != nil {
		panic(err)
	}
	// 小于4字节说明连前面的校验和都没有
	if size < 4 {
		panic(errors.New(commons.ErrorMessage.BadLogFileError))
	}

	// 获取前面4字节的校验和
	raw := make([]byte, 4)
	_, err = logger.file.ReadAt(raw, 0)
	if err != nil {
		panic(err)
	}

	checkSum := int32(binary.BigEndian.Uint32(raw))
	logger.fileSize = size
	logger.xCheckSum = checkSum
	// 检查校验和并且移除后面的截断部分
	logger.checkAndRemoveTail()
}

// 对校验和进行检查并且移除后面的截断部分
func (logger *DBLogger) checkAndRemoveTail() {
	logger.Rewind()

	var xCheck int32 = 0
	// 从头开始读取，计算校验和
	for {
		// 读取下一条日志
		log := logger.internNext()
		if log == nil {
			break
		}
		// 计算当前这条日志的校验和
		xCheck = logger.calCheckSum(xCheck, log)
	}
	if xCheck != logger.xCheckSum {
		panic(errors.New(commons.ErrorMessage.BadLogFileError))
	}

	// 截断后面的部分
	err := logger.Truncate(logger.currentPosition)
	if err != nil {
		panic(err)
	}

	_, err = logger.file.Seek(logger.currentPosition, 1)
	if err != nil {
		panic(err)
	}

	// 重置文件指针位置
	logger.Rewind()
}

// calCheckSum 计算校验和
func (logger *DBLogger) calCheckSum(xCheck int32, log []byte) int32 {
	for _, b := range log {
		xCheck = xCheck*SEED + int32(int8(b))
	}
	return xCheck
}

// Log 记录日志
func (logger *DBLogger) Log(data []byte) {
	logger.lock.Lock()
	defer logger.lock.Unlock()

	// 将数据包装成日志条目
	log := logger.wrapLog(data)
	stat, err2 := logger.file.Stat()
	if err2 != nil {
		panic(err2)
	}
	// 写入日志
	_, err := logger.file.WriteAt(log, stat.Size())

	if err != nil {
		panic(err)
	}
	logger.file.Sync()

	// 更新校验和
	logger.updateXCheckSum(log)

}

// updateXCheckSum 更新校验和
func (logger *DBLogger) updateXCheckSum(log []byte) {
	logger.xCheckSum = int32(logger.calCheckSum(logger.xCheckSum, log))
	checkSum := make([]byte, 4)
	binary.BigEndian.PutUint32(checkSum, uint32(logger.xCheckSum))
	_, err := logger.file.WriteAt(checkSum, 0)
	if err != nil {
		panic(err)
	}
	logger.file.Sync()

}

// wrapLog 将数据包装成日志条目
func (logger *DBLogger) wrapLog(data []byte) []byte {
	checkSum := make([]byte, 4)
	binary.BigEndian.PutUint32(checkSum, uint32(logger.calCheckSum(0, data)))
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(data)))
	return append(append(size, checkSum...), data...)
}

// Truncate 截断文件
func (logger *DBLogger) Truncate(x int64) error {
	logger.lock.Lock()
	defer logger.lock.Unlock()

	return logger.file.Truncate(x)
}

// internNext 读取下一个日志条目
func (logger *DBLogger) internNext() []byte {
	// 如果当前文件指针位置 + 8字节 大于等于了文件大小，直接返回
	// 这里8字节是因为每条日志内部的前边4个字节是size，接着4个字节是检验和，再往后才是数据
	if logger.currentPosition+int64(OffsetDataSize) >= logger.fileSize {
		return nil
	}

	tmp := make([]byte, LogItemLengthSize)
	_, err := logger.file.ReadAt(tmp, logger.currentPosition)
	if err != nil {
		panic(err)
	}
	size := int(binary.BigEndian.Uint32(tmp))
	// 注意这里size只是表示后面的data的长度，而不是整个日志条目的长度
	if logger.currentPosition+int64(size)+int64(OffsetDataSize) > logger.fileSize {
		return nil
	}

	// 读取整条日志记录，包括了前面的8字节数据
	buf := make([]byte, size+OffsetDataSize)
	_, err = logger.file.ReadAt(buf, logger.currentPosition)
	if err != nil {
		panic(err)
	}

	// 根据后面的data的内容去获得一个校验和
	checkSum1 := logger.calCheckSum(0, buf[OffsetDataSize:])
	// 读取前面的校验和
	checkSum2 := int32(binary.BigEndian.Uint32(buf[OffsetCheckSumSize:OffsetDataSize]))
	// 如果校验和不相等，直接返回
	if checkSum1 != checkSum2 {
		return nil
	}
	logger.currentPosition += int64(size + OffsetDataSize)
	return buf
}

func (logger *DBLogger) Next() []byte {
	logger.lock.Lock()
	defer logger.lock.Unlock()

	log := logger.internNext()
	if log == nil {
		return nil
	}
	return log[OffsetDataSize:]
}

// Rewind 将文件指针位置重新定位到最开始的校验和后面，即4字节的位置
func (logger *DBLogger) Rewind() {
	logger.currentPosition = int64(OffsetCheckSumSize)
}

// Close 关闭文件
func (logger *DBLogger) Close() {
	err := logger.file.Close()
	if err != nil {
		panic(err)
	}
}
