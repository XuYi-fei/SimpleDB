package dmPage

//var (
//	PageSize = 1 << 13
//)
//
//// Page 页面实现
//type Page struct {
//	pageNumber int
//	data       []byte
//	dirty      bool
//	mu         commons.ReentrantLock
//
//	pageCache *dmPageCache.PageCache
//}
//
//func NewPageImpl(pageNumber int, data []byte, pc *dmPageCache.PageCache) *Page {
//	return &Page{
//		pageNumber: pageNumber,
//		data:       data,
//		dirty:      false,
//		//mu:         commons.ReentrantLock{},
//		pageCache: pc,
//	}
//}
//
//// Lock 加锁
//func (pageImpl *Page) Lock() {
//	pageImpl.mu.Lock()
//}
//
//// Unlock 解锁
//func (pageImpl *Page) Unlock() {
//	pageImpl.mu.Unlock()
//}
//
//func (pageImpl *Page) Release() {
//	pageImpl.pageCache.Release(pageImpl)
//}
//
//func (pageImpl *Page) SetDirty(dirty bool) {
//	pageImpl.dirty = dirty
//}
//
//func (pageImpl *Page) IsDirty() bool {
//	return pageImpl.dirty
//}
//
//func (pageImpl *Page) GetPageNumber() int {
//	return pageImpl.pageNumber
//}
//
//func (pageImpl *Page) GetData() []byte {
//	return pageImpl.data
//}
