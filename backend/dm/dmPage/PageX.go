package dmPage

import (
	"dbofmine/backend/dm/constants"
	"encoding/binary"
)

type PageX struct {
}

var (
	PageXOffsetFreeSpace int32 = 0
	PageXOffsetDataSize  int32 = 2
	PageXMaxFreeSpace          = constants.PageSize - int(PageXOffsetDataSize)
)

func PageXInitRaw() []byte {
	data := make([]byte, constants.PageSize)
	PageXSetFreeSpaceOffset(data, int16(PageXOffsetDataSize))
	return data
}

func PageXSetFreeSpaceOffset(raw []byte, offsetData int16) {
	binary.BigEndian.PutUint16(raw[int(PageXOffsetFreeSpace):int(PageXOffsetDataSize)], uint16(offsetData))
}

// PageXGetPageFreeSpaceOffset 获得页面当前的空闲位置的起始偏移量
func PageXGetPageFreeSpaceOffset(page *Page) int16 {
	return PageXGetFreeSpaceOffset(page.GetData())
}

// PageXGetFreeSpaceOffset 根据原始数据转换获得空闲位置的起始偏移量
func PageXGetFreeSpaceOffset(raw []byte) int16 {
	return int16(binary.BigEndian.Uint16(raw[0:PageXOffsetDataSize]))
}

// InsertData2PageX 向页面中插入数据data，返回插入位置
func InsertData2PageX(page *Page, data []byte) int16 {
	page.SetDirty(true)
	// 获取页面的空闲位置偏移量
	offset := PageXGetFreeSpaceOffset(page.GetData())
	pageData := page.GetData()
	// 将data数据复制到页中的空闲位置
	copy(pageData[offset:offset+int16(len(data))], data)
	// 更新新的空闲位置
	PageXSetFreeSpaceOffset(pageData, offset+int16(len(data)))

	return offset
}

// PageXGetFreeSpace 获得页面的剩余空间
func PageXGetFreeSpace(page *Page) int32 {
	return int32(constants.PageSize) - int32(PageXGetFreeSpaceOffset(page.GetData()))
}

// PageXRecoverInsert 恢复插入数据
func PageXRecoverInsert(page *Page, raw []byte, offset int16) {
	page.SetDirty(true)
	copy(page.GetData()[offset:offset+int16(len(raw))], raw)
	spaceOffset := PageXGetFreeSpaceOffset(page.GetData())
	if spaceOffset < offset+int16(len(raw)) {
		PageXSetFreeSpaceOffset(page.GetData(), offset+int16(len(raw)))
	}
}

// PageXRecoverUpdate 恢复更新数据
func PageXRecoverUpdate(page *Page, raw []byte, offset int32) {
	page.SetDirty(true)
	copy(page.GetData()[offset:offset+int32(len(raw))], raw)
}
