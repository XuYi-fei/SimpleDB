package dmPage

import (
	"SimpleDB/backend/common"
	"SimpleDB/backend/dm/constants"
	"SimpleDB/backend/utils"
	"SimpleDB/commons"
	"os"
)

// Page 页面实现
type Page struct {
	pageNumber int
	data       []byte
	dirty      bool
	mu         commons.ReentrantLock

	pageCache *PageCache
}

func NewPage(pageNumber int, data []byte, pc *PageCache) *Page {
	return &Page{
		pageNumber: pageNumber,
		data:       data,
		dirty:      false,
		//mu:         commons.ReentrantLock{},
		pageCache: pc,
	}
}

// Lock 加锁
func (page *Page) Lock() {
	page.mu.Lock()
}

// Unlock 解锁
func (page *Page) Unlock() {
	page.mu.Unlock()
}

func (page *Page) Release() {
	page.pageCache.Release(page)
}

func (page *Page) SetDirty(dirty bool) {
	page.dirty = dirty
}

func (page *Page) IsDirty() bool {
	return page.dirty
}

func (page *Page) GetPageNumber() int {
	return page.pageNumber
}

func (page *Page) GetData() []byte {
	return page.data
}

var (
	MEM_MIN_LIM = 10
	DB_SUFFIX   = ".db"
)

type PageCache struct {
	file *os.File
	// 需要原子操作页数
	pageNumbers int32
	// 可重入锁
	lock commons.ReentrantLock
	// 抽象缓存类
	CacheManager *common.AbstractCache[*Page]
}

// CreatePageCache 创建页面缓存
func CreatePageCache(path string, memory int64) *PageCache {
	file, err := os.OpenFile(path+DB_SUFFIX, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

	// 获取文件长度
	fileLength, _ := utils.GetFileSizeByPath(path + DB_SUFFIX)
	pageCache := PageCache{
		file:         file,
		pageNumbers:  int32(int(fileLength / int64(constants.PageSize))),
		lock:         commons.ReentrantLock{},
		CacheManager: nil,
	}

	// 计算最大缓存数量
	maxResource := int(memory / int64(constants.PageSize))
	if maxResource < MEM_MIN_LIM {
		panic(commons.ErrorMessage.AllocMemoryTooSmallError)
	}
	cache := common.NewAbstractCache[*Page](maxResource, &pageCache)

	// 把抽象缓存创建出来赋值
	pageCache.CacheManager = cache

	return &pageCache
}

// OpenPageCache 打开页面缓存
func OpenPageCache(path string, memory int64) *PageCache {
	file, _ := os.OpenFile(path+DB_SUFFIX, os.O_RDWR, 0755)

	// 获取文件长度
	fileLength, _ := utils.GetFileSizeByPath(path + DB_SUFFIX)
	pageCache := PageCache{
		file:         file,
		pageNumbers:  int32(int(fileLength / int64(constants.PageSize))),
		lock:         commons.ReentrantLock{},
		CacheManager: nil,
	}

	// 计算最大缓存数量
	maxResource := int(memory / int64(constants.PageSize))
	cache := common.NewAbstractCache[*Page](maxResource, &pageCache)

	// 把抽象缓存创建出来赋值
	pageCache.CacheManager = cache

	return &pageCache
}
