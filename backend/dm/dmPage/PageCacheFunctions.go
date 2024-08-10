package dmPage

import (
	"SimpleDB/backend/dm/constants"
	"sync/atomic"
)

// GetForCache 实现抽象缓存接口
func (pageCache *PageCache) GetForCache(key int64) (*Page, error) {
	pageNo := int(key)
	offset := pageCache.pageOffset(pageNo)

	buf := make([]byte, constants.PageSize)
	// 加锁，准备获取页面数据
	pageCache.lock.Lock()
	defer pageCache.lock.Unlock()

	_, err := pageCache.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	page := NewPage(pageNo, buf, pageCache)

	return page, nil
}

// ReleaseForCache 实现抽象缓存接口
func (pageCache *PageCache) ReleaseForCache(pg *Page) {
	if pg.IsDirty() {
		pageCache.flush(pg)
		pg.SetDirty(false)
	}
}

// Release 释放页面
func (pageCache *PageCache) Release(page *Page) {
	pageCache.CacheManager.Release(int64(page.GetPageNumber()))
}

// NewPage 创建新页面，返回创建的页的页码
func (pageCache *PageCache) NewPage(initData []byte) int {
	pageNumber := atomic.AddInt32(&pageCache.pageNumbers, 1)
	pg := NewPage(int(pageNumber), initData, nil)
	pageCache.flush(pg)
	return int(pageNumber)
}

// GetPage 获取页面
func (pageCache *PageCache) GetPage(pageNumber int) (*Page, error) {
	return pageCache.CacheManager.Get(int64(pageNumber))
}

// FlushPage 刷新页面
func (pageCache *PageCache) FlushPage(pg *Page) {
	pageCache.flush(pg)
}

// flush 真正刷新
func (pageCache *PageCache) flush(pg *Page) {
	pageNo := (*pg).GetPageNumber()
	offset := pageCache.pageOffset(pageNo)

	pageCache.lock.Lock()
	defer pageCache.lock.Unlock()

	// 写入数据
	pageCache.file.WriteAt((*pg).GetData(), offset)
	// 刷新磁盘
	pageCache.file.Sync()

}

// TruncateByPgNo 截断文件，保留指定页数
func (pageCache *PageCache) TruncateByPgNo(maxPageNumber int) {
	size := pageCache.pageOffset(maxPageNumber + 1)
	pageCache.file.Truncate(size)
	atomic.StoreInt32(&pageCache.pageNumbers, int32(maxPageNumber))
}

// Close 关闭文件
func (pageCache *PageCache) Close() {
	pageCache.CacheManager.Close()
	err := pageCache.file.Close()
	if err != nil {
		panic(err)
	}
}

func (pageCache *PageCache) GetPageNumber() int {
	return int(atomic.LoadInt32(&pageCache.pageNumbers))
}

func (pageCache *PageCache) pageOffset(pageNo int) int64 {
	return int64((pageNo - 1) * constants.PageSize)
}
