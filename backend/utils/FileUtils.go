package utils

import "os"

// FileExists 判断文件是否存在
func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// GetFileSizeByPath 获取文件大小（字节数）
func GetFileSizeByPath(filename string) (int64, error) {
	// 使用 os.Stat 获取文件信息
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}

	// 使用 Size 方法获取文件大小
	return fileInfo.Size(), nil
}

// GetFileSize 获取文件大小（字节数）
func GetFileSize(file *os.File) (int64, error) {
	// 使用 os.Stat 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, err
	}

	// 使用 Size 方法获取文件大小
	return fileInfo.Size(), nil
}
