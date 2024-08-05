package vm

import (
	"dbofmine/backend/dm"
	"dbofmine/commons"
	"encoding/binary"
)

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [data]
 */
var (
	// EntryOffsetXMIN 定义了XMIN的偏移量为0
	EntryOffsetXMIN = 0
	// EntryOffsetXMAX 定义了XMAX的偏移量为XMIN偏移量后的8个字节
	EntryOffsetXMAX = EntryOffsetXMIN + 8
	// EntryOffsetData 定义了DATA的偏移量为XMAX偏移量后的8个字节
	EntryOffsetData = EntryOffsetXMAX + 8
)

type Entry struct {
	uid int64
	// dataItem DataItem对象，用来存储数据的
	dataItem *dm.DataItem
	// vm VersionManager对象，用来管理版本的
	vm *VersionManager
}

// ================= 实例方法 =================

func (entry *Entry) Release() {
	entry.vm.ReleaseEntry(entry)
}

func (entry *Entry) Remove() {
	entry.dataItem.Release()
}

// Data 以拷贝的形式返回内容
func (entry *Entry) Data() []byte {
	entry.dataItem.RLock()
	defer entry.dataItem.RUnLock()
	// 获取日志数据
	sa := entry.dataItem.Data()
	// 创建一个去除前16字节的数组，因为前16字节表示 xmin and xmax
	data := make([]byte, len(sa)-EntryOffsetData)
	// 拷贝数据到data数组上
	copy(data, sa[EntryOffsetData:])

	return data
}

func (entry *Entry) GetXMin() int64 {
	entry.dataItem.RLock()
	defer entry.dataItem.RUnLock()
	return int64(binary.BigEndian.Uint64(entry.dataItem.Data()[EntryOffsetXMIN:EntryOffsetXMAX]))
}

func (entry *Entry) GetXMax() int64 {
	entry.dataItem.RLock()
	defer entry.dataItem.RUnLock()
	return int64(binary.BigEndian.Uint64(entry.dataItem.Data()[EntryOffsetXMAX:EntryOffsetData]))
}

// SetXMax 设置删除版本的事务编号
func (entry *Entry) SetXMax(xid int64) {
	// 在修改或删除之前先拷贝好旧数值
	entry.dataItem.Before()
	// 生成一个修改日志
	defer entry.dataItem.After(xid)

	sa := entry.dataItem.Data()
	copy(sa[EntryOffsetXMAX:EntryOffsetData], commons.Int64ToBytes(xid))
}

func (entry *Entry) GetUid() int64 {
	return entry.uid
}

// ================= 静态方法 =================

// NewEntry 创建一个新的Entry对象
func NewEntry(vm *VersionManager, dataItem *dm.DataItem, uid int64) *Entry {
	if dataItem == nil {
		return nil
	}

	entry := &Entry{
		uid:      uid,
		dataItem: dataItem,
		vm:       vm,
	}
	return entry
}

// LoadEntry 用来加载一个Entry。它首先从VersionManager中读取数据，然后创建一个新的Entry
func LoadEntry(vm *VersionManager, uid int64) *Entry {
	dataItem := vm.DM.Read(uid)
	return NewEntry(vm, dataItem, uid)
}

// WrapEntryRaw 生成日志格式的Entry数据
func WrapEntryRaw(xid int64, data []byte) []byte {
	// 将事务id转为8字节数组
	xminBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(xminBytes, uint64(xid))
	// 创建一个空的8字节数组，等待版本修改或删除时才修改
	xmaxBytes := make([]byte, 8)
	// 将XMIN和XMAX拼接到一起，然后拼接上data
	return commons.BytesConcat(xminBytes, xmaxBytes, data)
}
