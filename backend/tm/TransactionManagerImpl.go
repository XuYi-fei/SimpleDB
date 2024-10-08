package tm

import (
	"SimpleDB/backend/utils"
	"SimpleDB/commons"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// LenXidHeaderLength XID文件头长度
	LenXidHeaderLength int64 = 8
	// LenXidFieldSize 每个事务的占用长度
	LenXidFieldSize int64 = 1

	// FieldTranActive 事务的三种状态
	FieldTranActive    byte = 0
	FieldTranCommitted byte = 1
	FieldTranAborted   byte = 2

	// SuperXid 超级事务，永远为commited状态
	SuperXid int64 = 0

	// XidSuffix 事务文件后缀
	XidSuffix = ".xid"
)

type TransactionManagerImpl struct {
	file *os.File
	// xidCounter 事务id的编号，同时也是计数器
	xidCounter int64
	// counterLock 用于保护xidCounter，自动初始化，不用手动赋值
	counterLock sync.Mutex
}

// CreateTransactionManagerImpl 创建一个新的事务管理器
func CreateTransactionManagerImpl(path string) (*TransactionManagerImpl, error) {
	var transactionManager *TransactionManagerImpl
	// 如果文件已经存在那么直接报错
	if utils.FileExists(path + XidSuffix) {
		panic(commons.ErrorMessage.FileExistError)
	}
	// 尝试打开文件
	file, err := os.OpenFile(path+XidSuffix, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	} else {
		// 创建新的事务管理器TM
		transactionManager = &TransactionManagerImpl{
			file: file,
		}
	}

	// 写空XID文件头
	blankHeader := make([]byte, LenXidHeaderLength)
	writeNum, err := transactionManager.file.WriteAt(blankHeader, 0)
	if err != nil {
		return nil, err
	}
	if int64(writeNum) != LenXidHeaderLength {
		panic(commons.ErrorMessage.WriteFileHeaderError)
	}
	return transactionManager, nil
}

func OpenTransactionManagerImpl(path string) (*TransactionManagerImpl, error) {
	// 如果文件不存在那么直接报错
	if !utils.FileExists(path + XidSuffix) {
		panic(commons.ErrorMessage.FileExistError)
	}
	// 尝试打开文件
	file, err := os.OpenFile(path+XidSuffix, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	// 创建新的事务管理器TM
	transactionManager := &TransactionManagerImpl{
		file: file,
	}
	// 检查XID计数器是否合法
	transactionManager.checkXidCounter()
	commons.Logger.Debugf("xid文件校验成功!")

	return transactionManager, nil
}

// checkXidCounter 检查XID计数器是否合法
// 读取XID_FILE_HEADER中的xidCounter，根据它计算文件的理论长度，对比实际长度
func (manager *TransactionManagerImpl) checkXidCounter() {
	// 获取文件长度
	fileLength, _ := utils.GetFileSizeByPath(manager.file.Name())
	if fileLength < LenXidHeaderLength {
		panic(commons.ErrorMessage.BadXIDFileException)
	}
	// 读取xid头标识的文件长度
	xidHeader := make([]byte, LenXidHeaderLength)
	_, _ = manager.file.ReadAt(xidHeader, 0)
	manager.xidCounter = int64(binary.BigEndian.Uint64(xidHeader))

	if fileLength != manager.getXidPosition(manager.xidCounter+1) {
		panic(commons.ErrorMessage.BadXIDFileException)
	}

}

// getXidPosition 根据事务xid取得其在xid文件中对应的位置
func (manager *TransactionManagerImpl) getXidPosition(xid int64) int64 {
	return LenXidHeaderLength + (xid-1)*LenXidFieldSize
}

// updateXID 更新xid事务的状态为status
func (manager *TransactionManagerImpl) updateXID(xid int64, status byte) {
	offset := manager.getXidPosition(xid)
	// 创建事务的状态的字节数组
	tmp := make([]byte, LenXidFieldSize)
	tmp[0] = status
	// 写入事务状态
	_, err := manager.file.WriteAt(tmp[:], offset)
	if err != nil {
		panic(err)
	}

	// 强制刷新
	err = manager.file.Sync()
	if err != nil {
		panic(err)
	}
}

// incrXIDCounter 增加xidCounter， 并更新XID文件的头部信息
func (manager *TransactionManagerImpl) incrXIDCounter() {
	// 事务总数加一
	manager.xidCounter++
	// 创建一个长度为8的字节数组
	tmp := make([]byte, LenXidHeaderLength)
	// 将xidCounter转换为字节数组
	binary.BigEndian.PutUint64(tmp, uint64(manager.xidCounter))
	// 将xidCounter写入文件
	_, err := manager.file.WriteAt(tmp[:], 0)
	if err != nil {
		panic(err)
	}

	// 强制刷新
	err = manager.file.Sync()
	if err != nil {
		panic(err)
	}
}

// Begin 开启一个事务
func (manager *TransactionManagerImpl) Begin() int64 {
	manager.counterLock.Lock()

	// 更新xidCounter
	xid := manager.xidCounter + 1
	// 更新事务状态
	manager.updateXID(xid, FieldTranActive)
	// 调用incrXIDCounter方法，将事务计数器加1，并更新XID文件的头部信息
	manager.incrXIDCounter()
	defer manager.counterLock.Unlock()
	return xid
}

// Commit 提交一个事务
func (manager *TransactionManagerImpl) Commit(xid int64) {
	manager.updateXID(xid, FieldTranCommitted)
}

// Abort 终止一个事务
func (manager *TransactionManagerImpl) Abort(xid int64) {
	manager.updateXID(xid, FieldTranAborted)
}

// CheckXID 检查事务的状态，判断xid对应的事务是否处于status状态
func (manager *TransactionManagerImpl) CheckXID(xid int64, status byte) bool {
	offset := manager.getXidPosition(xid)
	// 创建一个长度为8的字节数组
	buf := make([]byte, LenXidFieldSize)
	// 读取事务状态
	_, err := manager.file.ReadAt(buf, offset)
	if err != nil {
		panic(err)
	}

	return buf[0] == status
}

// IsActive 判断事务是否处于活动状态
func (manager *TransactionManagerImpl) IsActive(xid int64) bool {
	if xid == SuperXid {
		return false
	}
	return manager.CheckXID(xid, FieldTranActive)
}

// IsCommitted 判断事务是否处于提交状态
func (manager *TransactionManagerImpl) IsCommitted(xid int64) bool {
	if xid == SuperXid {
		return true
	}
	return manager.CheckXID(xid, FieldTranCommitted)
}

// IsAborted 判断事务是否处于终止状态
func (manager *TransactionManagerImpl) IsAborted(xid int64) bool {
	if xid == SuperXid {
		return false
	}
	return manager.CheckXID(xid, FieldTranAborted)
}

// Close 关闭事务管理器
func (manager *TransactionManagerImpl) Close() {
	err := manager.file.Close()
	if err != nil {
		panic(err)
	}
}
