package dmPage

import "SimpleDB/commons"

type MockPage struct {
	pageNumber int
	data       []byte
	mu         commons.ReentrantLock
}

func NewMockPage(pageNumber int, data []byte) *MockPage {
	return &MockPage{
		pageNumber: pageNumber,
		data:       data,
	}
}

// lock 加锁
func (pageImpl *MockPage) Lock() {
	pageImpl.mu.Lock()
}

// unlock 解锁
func (pageImpl *MockPage) Unlock() {
	pageImpl.mu.Unlock()
}

func (pageImpl *MockPage) Release() {
}

func (pageImpl *MockPage) SetDirty(dirty bool) {

}

func (pageImpl *MockPage) IsDirty() bool {
	return false
}

func (pageImpl *MockPage) GetPageNumber() int {
	return pageImpl.pageNumber
}

func (pageImpl *MockPage) GetData() []byte {
	return pageImpl.data
}
