package dmPageCache

import (
	"dbofmine/backend/common"
	"dbofmine/backend/utils"
	"dbofmine/commons"
	"os"
)

var (
	PageSize = 1 << 13
)

// Page 页面实现
type Page struct {
	pageNumber int
	data       []byte
	dirty      bool
	mu         commons.ReentrantLock

	pageCache *PageCache
}

func NewPageImpl(pageNumber int, data []byte, pc *PageCache) *Page {
	return &Page{
		pageNumber: pageNumber,
		data:       data,
		dirty:      false,
		//mu:         commons.ReentrantLock{},
		pageCache: pc,
	}
}

// Lock 加锁
func (pageImpl *Page) Lock() {
	pageImpl.mu.Lock()
}

// Unlock 解锁
func (pageImpl *Page) Unlock() {
	pageImpl.mu.Unlock()
}

func (pageImpl *Page) Release() {
	pageImpl.pageCache.Release(pageImpl)
}

func (pageImpl *Page) SetDirty(dirty bool) {
	pageImpl.dirty = dirty
}

func (pageImpl *Page) IsDirty() bool {
	return pageImpl.dirty
}

func (pageImpl *Page) GetPageNumber() int {
	return pageImpl.pageNumber
}

func (pageImpl *Page) GetData() []byte {
	return pageImpl.data
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
	cacheManager *common.AbstractCache[*Page]
}

// NewPageCacheImpl 创建页面缓存
func NewPageCacheImpl(path string, memory int64) *PageCache {
	file, err := os.OpenFile(path+DB_SUFFIX, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

	// 获取文件长度
	fileLength, _ := utils.GetFileSize(path + DB_SUFFIX)
	pageCacheImpl := PageCache{
		file:         file,
		pageNumbers:  int32(int(fileLength / int64(PageSize))),
		lock:         commons.ReentrantLock{},
		cacheManager: nil,
	}

	// 计算最大缓存数量
	maxResource := int(memory / int64(PageSize))
	cache := common.NewAbstractCache[*Page](maxResource, &pageCacheImpl)

	// 把抽象缓存创建出来赋值
	pageCacheImpl.cacheManager = cache

	return &pageCacheImpl
}

// OpenPageCacheImpl 打开页面缓存
func OpenPageCacheImpl(path string, memory int64) *PageCache {
	file, _ := os.OpenFile(path+DB_SUFFIX, os.O_RDWR, 0755)

	// 获取文件长度
	fileLength, _ := utils.GetFileSize(path + DB_SUFFIX)
	pageCacheImpl := PageCache{
		file:         file,
		pageNumbers:  int32(int(fileLength / int64(PageSize))),
		lock:         commons.ReentrantLock{},
		cacheManager: nil,
	}

	// 计算最大缓存数量
	maxResource := int(memory / int64(PageSize))
	cache := common.NewAbstractCache[*Page](maxResource, &pageCacheImpl)

	// 把抽象缓存创建出来赋值
	pageCacheImpl.cacheManager = cache

	return &pageCacheImpl
}
