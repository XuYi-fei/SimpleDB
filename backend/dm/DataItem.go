package dm

import (
	"SimpleDB/backend/dm/dmPage"
	"SimpleDB/backend/utils"
	"encoding/binary"
	"sync"
)

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 * UID 结构如下:
 * [pageNumber] [空] [offset]
 * pageNumber 4字节，页号
 * 中间空下2字节
 * offset 2字节，偏移量
 */
var (
	// DataItemOffsetValid 数据项的校验位置
	DataItemOffsetValid = 0

	// DataItemOffsetDataSize 数据项的数据大小位置
	DataItemOffsetDataSize = 1

	// DataItemOffsetData 数据项的数据位置
	DataItemOffsetData = 3
)

type DataItem struct {
	// 数据
	raw []byte

	oldRaw []byte

	// lock TODO 读写锁，是否需要可重入锁还不清楚
	lock sync.RWMutex

	dataManager *DataManager

	uid  int64
	page *dmPage.Page
}

func NewDataItem(raw []byte, oldRaw []byte, page *dmPage.Page, uid int64, dataManager *DataManager) *DataItem {
	return &DataItem{
		raw:         raw,
		oldRaw:      oldRaw,
		page:        page,
		dataManager: dataManager,
		uid:         uid,
	}
}

// IsValid 判断数据项是否合法，注意0是合法标志
func (dataItem *DataItem) IsValid() bool {
	return dataItem.raw[DataItemOffsetValid] == 0
}

// Data 返回数据项的数据
func (dataItem *DataItem) Data() []byte {
	return dataItem.raw[DataItemOffsetData:]
}

// Before 在修改数据项之前调用
func (dataItem *DataItem) Before() {
	dataItem.lock.Lock()
	dataItem.page.SetDirty(true)
	dataItem.oldRaw = make([]byte, len(dataItem.raw))
	copy(dataItem.oldRaw, dataItem.raw)
}

// UnBefore 撤销修改数据项之前的操作
func (dataItem *DataItem) UnBefore() {
	dataItem.raw = make([]byte, len(dataItem.oldRaw))
	copy(dataItem.raw, dataItem.oldRaw)
	dataItem.lock.Unlock()
}

// After 在修改数据项之后调用
func (dataItem *DataItem) After(xid int64) {
	dataItem.dataManager.LogDataItem(xid, dataItem)
	dataItem.lock.Unlock()
}

// Release 释放数据项
func (dataItem *DataItem) Release() {
	dataItem.dataManager.ReleaseDataItem(dataItem)
}

func (dataItem *DataItem) Lock() {
	dataItem.lock.Lock()
}

func (dataItem *DataItem) UnLock() {
	dataItem.lock.Unlock()
}

func (dataItem *DataItem) RLock() {
	dataItem.lock.RLock()
}

func (dataItem *DataItem) RUnLock() {
	dataItem.lock.RUnlock()
}

// Page 返回数据项所在的页
func (dataItem *DataItem) Page() *dmPage.Page {
	return dataItem.page
}

// UID 返回数据项的UID
func (dataItem *DataItem) UID() int64 {
	return dataItem.uid
}

func (dataItem *DataItem) GetOldRaw() []byte {
	return dataItem.oldRaw
}

// GetRaw 返回数据项的原始数据
func (dataItem *DataItem) GetRaw() []byte {
	return dataItem.raw
}

// WrapDataItemRaw 包装数据项，将标志位和长度加到原始数据的前面，从而返回符合DataItem格式的数据
func WrapDataItemRaw(raw []byte) []byte {
	valid := make([]byte, 1)
	size := make([]byte, 2)
	wrappedData := make([]byte, len(raw)+3)
	wrappedData[0] = valid[0]
	binary.BigEndian.PutUint16(size, uint16(len(raw)))
	wrappedData[1] = size[0]
	wrappedData[2] = size[1]
	copy(wrappedData[3:], raw)
	return wrappedData
}

// ParseDataItem 解析page页中的数据从而得到数据项
func ParseDataItem(page *dmPage.Page, offset int32, dataManager *DataManager) *DataItem {
	// 获取该页的字节数据
	raw := page.GetData()
	// 从offset开始解析数据，要解析出一个dataItem
	// 先从offset开始解析dataItem的size
	size := binary.BigEndian.Uint16(raw[int(offset)+DataItemOffsetDataSize : int(offset)+DataItemOffsetData])
	// size能够得出dataItem的长度，现在需要将dataItem在页中的数据所处的位置解析出来
	// length对应的是dataItem的长度，加上offset就是dataItem的结束位置
	length := int(size) + DataItemOffsetData
	// 生成UID
	uid := utils.GenerateUID(page.GetPageNumber(), int(offset))
	// 生成dataItem
	dataItem := NewDataItem(raw[offset:int(offset)+length], make([]byte, length), page, uid, dataManager)
	return dataItem
}

// SetDataItemRawInValid 设置数据项为失效
func SetDataItemRawInValid(raw []byte) {
	raw[DataItemOffsetValid] = 1
}
