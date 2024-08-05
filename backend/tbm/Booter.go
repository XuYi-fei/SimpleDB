package tbm

import (
	"SimpleDB/backend/utils"
	"fmt"
	"os"
)

// 记录第一个表的UID

var (
	// BooterSuffix 数据库启动信息文件的后缀
	BooterSuffix = ".bt"
	// BooterTmpSuffix 数据库启动信息文件的临时后缀
	BooterTmpSuffix = ".bt_tmp"
)

type Booter struct {
	// 数据库启动信息文件的路径
	Path string
	// 数据库启动信息文件
	file *os.File
}

// Load 加载文件启动信息文件
func (b *Booter) Load() []byte {
	fileSize, err := utils.GetFileSize(b.file)
	if err != nil {
		panic(err)
	}
	// 创建一个大小为文件大小的字节数组
	data := make([]byte, fileSize)
	// 读取文件的所有字节到data中
	if _, err := b.file.ReadAt(data, 0); err != nil {
		panic(err)
	}
	return data
}

// Update 更新启动信息文件的内容
func (b *Booter) Update(data []byte) {
	// 创建一个新的临时文件
	tmpFile, err := os.Create(b.Path + BooterTmpSuffix)
	if err != nil {
		panic(err)
	}
	// 检查文件是否可读写，如果不可读写，则抛出异常
	if err := tmpFile.Chmod(0666); err != nil {
		panic(err)
	}
	// 将data写入临时文件
	if _, err := tmpFile.Write(data); err != nil {
		panic(err)
	}
	// 立刻将数据刷新到磁盘
	err = tmpFile.Sync()
	if err != nil {
		panic(err)
	}

	// 检查之前的启动信息文件是否存在
	if _, err := os.Stat(b.Path + BooterSuffix); err == nil {
		// 之前的启动信息文件存在，删除它
		err = os.Remove(b.Path + BooterSuffix)
		if err != nil {
			panic(err)
		}
	}
	// 将临时文件移动到启动信息文件的位置，替换原来的文件
	if err := os.Rename(b.Path+BooterTmpSuffix, b.Path+BooterSuffix); err != nil {
		panic(err)
	}

	// 重新打开目标文件
	file, err := os.OpenFile(b.Path+BooterSuffix, os.O_RDWR, 0666)
	if err != nil {
		panic(fmt.Sprintf("Failed to open file: %v", err))
	}

	// 更新file字段为新的启动信息文件
	b.file = file
	// 检查新的启动信息文件是否可读写，如果不可读写，则抛出异常
	if err := b.file.Chmod(0666); err != nil {
		panic(err)
	}
}

// CreateBooter 创建一个新的Booter对象
func CreateBooter(path string) *Booter {
	// 删除可能存在的临时文件
	RemoveBatTmpBooter(path)
	// 创建一个新的文件对象，文件名是路径加上启动信息文件的后缀
	file, err := os.Create(path + BooterSuffix)
	if err != nil {
		panic(err)
	}
	// 检查文件是否可读写，如果不可读写，则抛出异常
	if err := file.Chmod(0666); err != nil {
		panic(err)
	}
	// 返回一个新的数据库启动信息对象
	return &Booter{
		Path: path,
		file: file,
	}
}

// OpenBooter 打开一个已经存在的Booter对象
func OpenBooter(path string) *Booter {
	// 删除可能存在的临时文件
	RemoveBatTmpBooter(path)
	// 创建一个新的文件对象，文件名是路径加上启动信息文件的后缀
	file, err := os.OpenFile(path+BooterSuffix, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	// 检查文件是否可读写，如果不可读写，则抛出异常
	if err := file.Chmod(0666); err != nil {
		panic(err)
	}
	// 返回一个新的数据库启动信息对象
	return &Booter{
		Path: path,
		file: file,
	}
}

// RemoveBatTmpBooter 删除可能存在的临时文件
func RemoveBatTmpBooter(path string) {
	// 删除路径加上临时文件后缀的文件
	os.RemoveAll(path + BooterTmpSuffix)
}
