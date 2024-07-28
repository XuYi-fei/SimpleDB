package dmPage

import (
	"dbofmine/backend/dm/constants"
	"sync/atomic"
)

// GetForCache 实现抽象缓存接口
func (pageCacheImpl *PageCache) GetForCache(key int64) (*Page, error) {
	pageNo := int(key)
	offset := pageCacheImpl.pageOffset(pageNo)

	buf := make([]byte, constants.PageSize)
	// 加锁，准备获取页面数据
	pageCacheImpl.lock.Lock()
	defer pageCacheImpl.lock.Unlock()

	_, err := pageCacheImpl.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	page := NewPageImpl(pageNo, buf, pageCacheImpl)

	return page, nil
}

func (pageCacheImpl *PageCache) ReleaseForCache(pg *Page) {
	if pg.IsDirty() {
		pageCacheImpl.Flush(pg)
		pg.SetDirty(false)
	}
}

func (pageCacheImpl *PageCache) Release(page *Page) {
	pageCacheImpl.cacheManager.Release(int64(page.GetPageNumber()))
}

// NewPage 创建新页面，返回创建的页的页码
func (pageCacheImpl *PageCache) NewPage(initData []byte) int {
	pageNumber := atomic.AddInt32(&pageCacheImpl.pageNumbers, 1)
	pg := NewPageImpl(int(pageNumber), initData, nil)
	pageCacheImpl.Flush(pg)
	return int(pageNumber)
}

// GetPage 获取页面
func (pageCacheImpl *PageCache) GetPage(pageNumber int) (*Page, error) {
	return pageCacheImpl.cacheManager.Get(int64(pageNumber))
}

// FlushPage 刷新页面
func (pageCacheImpl *PageCache) FlushPage(pg *Page) {
	pageCacheImpl.Flush(pg)
}

// Flush 真正刷新
func (pageCacheImpl *PageCache) Flush(pg *Page) {
	pageNo := (*pg).GetPageNumber()
	offset := pageCacheImpl.pageOffset(pageNo)

	pageCacheImpl.lock.Lock()
	defer pageCacheImpl.lock.Unlock()

	// 写入数据
	pageCacheImpl.file.WriteAt((*pg).GetData(), offset)
	// 刷新磁盘
	pageCacheImpl.file.Sync()

}

func (pageCacheImpl *PageCache) truncateByPgNo(maxPageNumber int) {
	size := pageCacheImpl.pageOffset(maxPageNumber + 1)
	pageCacheImpl.file.Truncate(size)
	atomic.StoreInt32(&pageCacheImpl.pageNumbers, int32(maxPageNumber))
}

func (pageCacheImpl *PageCache) Close() {
	pageCacheImpl.cacheManager.Close()
	err := pageCacheImpl.file.Close()
	if err != nil {
		panic(err)
	}
}

func (pageCacheImpl *PageCache) getPageNumber() int {
	return int(atomic.LoadInt32(&pageCacheImpl.pageNumbers))
}

func (pageCacheImpl *PageCache) pageOffset(pageNo int) int64 {
	return int64((pageNo - 1) * constants.PageSize)
}
