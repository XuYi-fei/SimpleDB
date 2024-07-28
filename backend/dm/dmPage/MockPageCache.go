package dmPage

import (
	"dbofmine/commons"
)

type MockPageCache struct {
	Cache map[int]*MockPage
	// 需要原子操作页数
	pageNumbers int
	// 可重入锁
	lock commons.ReentrantLock
	// 抽象缓存类
	//cacheManager *common.AbstractCache[*dmPage.Page]
}

func (pageCache *MockPageCache) NewPage(initData []byte) int {
	pageCache.lock.Lock()
	defer pageCache.lock.Unlock()

	pageNumber := pageCache.pageNumbers + 1
	newMockPage := NewMockPage(pageNumber, initData)
	pageCache.Cache[pageNumber] = newMockPage
	pageCache.pageNumbers++
	return pageNumber
}

func (pageCache *MockPageCache) GetPage(pageNumber int) *MockPage {
	pageCache.lock.Lock()
	defer pageCache.lock.Unlock()

	return pageCache.Cache[pageNumber]
}

func (pageCache *MockPageCache) Close() {

}

func (pageCache *MockPageCache) Release(page *MockPage) {

}

func (pageCache *MockPageCache) TruncateByPageNumber(maxPageNumber int) {

}

func (pageCache *MockPageCache) GetPageNumber() int {
	pageCache.lock.Lock()
	defer pageCache.lock.Unlock()
	return pageCache.pageNumbers
}

func (pageCache *MockPageCache) flushPage(pg *MockPage) {

}
