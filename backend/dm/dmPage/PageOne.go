package dmPage

import (
	"SimpleDB/backend/dm/constants"
	"SimpleDB/backend/utils"
)

var (
	// PageOneOffsetValidCheck 用于数据库文件中第一页的检查，偏移量为100的位置后的8+8个字节用来校验
	PageOneOffsetValidCheck = 100
	// PageOneLengthValidCheck 100字节后两个8字节用来校验，成功的情况下两个8字节应该一致
	PageOneLengthValidCheck = 8
)

type PageOne struct {
}

func PageOneInitRaw() []byte {
	raw := make([]byte, constants.PageSize)
	PageOneSetValidOpenData(raw)
	return raw
}

// PageOneSetValidStatusOpen 设置校验状态为真
func PageOneSetValidStatusOpen(page *Page) {
	page.SetDirty(true)
	PageOneSetValidOpenData(page.GetData())
}

func PageOneSetValidOpenData(data []byte) {
	// 生成校验数据
	randomBytes := utils.SafeRandomBytes(PageOneLengthValidCheck)
	copy(data[PageOneOffsetValidCheck:], randomBytes)
}

func PageOneSetValidStatusClose(page *Page) {
	page.SetDirty(false)
	PageOneSetValidCloseData(page.GetData())
}

func PageOneSetValidCloseData(data []byte) {
	// 生成校验数据
	copy(data[PageOneOffsetValidCheck+PageOneLengthValidCheck:PageOneOffsetValidCheck+2*PageOneLengthValidCheck], data[PageOneOffsetValidCheck:PageOneOffsetValidCheck+PageOneLengthValidCheck])
}

func CheckPageOneValid(page *Page) bool {
	return CheckPageOneDataValid(page.GetData())
}

// CheckPageOneDataValid 校验数据
func CheckPageOneDataValid(data []byte) bool {
	for i := 0; i < PageOneLengthValidCheck; i++ {
		if data[PageOneOffsetValidCheck+i] != data[PageOneOffsetValidCheck+PageOneLengthValidCheck+i] {
			return false
		}
	}
	return false
}
