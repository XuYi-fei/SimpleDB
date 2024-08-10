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

// PageOneInitRaw 初始化第一页的数据
func PageOneInitRaw() []byte {
	raw := make([]byte, constants.PageSize)
	PageOneSetValidOpenData(raw)
	return raw
}

// PageOneSetValidStatusOpen 设置校验状态为打开，即打开数据库时的状态
func PageOneSetValidStatusOpen(page *Page) {
	page.SetDirty(true)
	PageOneSetValidOpenData(page.GetData())
}

// PageOneSetValidOpenData 生成前半部分用来校验的随机字节
func PageOneSetValidOpenData(data []byte) {
	// 生成校验数据
	randomBytes := utils.SafeRandomBytes(PageOneLengthValidCheck)
	copy(data[PageOneOffsetValidCheck:], randomBytes)
}

// PageOneSetValidStatusClose 设置校验状态为关闭，即关闭数据库时的状态
func PageOneSetValidStatusClose(page *Page) {
	page.SetDirty(false)
	PageOneSetValidCloseData(page.GetData())
}

// PageOneSetValidCloseData 将校验数据复制到后半部分的字节
func PageOneSetValidCloseData(data []byte) {
	// 生成校验数据
	copy(data[PageOneOffsetValidCheck+PageOneLengthValidCheck:PageOneOffsetValidCheck+2*PageOneLengthValidCheck], data[PageOneOffsetValidCheck:PageOneOffsetValidCheck+PageOneLengthValidCheck])
}

// CheckPageOneValid 校验数据库文件中第一页的数据
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
