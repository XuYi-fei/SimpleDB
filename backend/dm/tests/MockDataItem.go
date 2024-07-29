package tests

import (
	"dbofmine/backend/dm/dmPage"
	"sync"
)

type MockDataItem struct {
	data    []byte
	oldData []byte
	uid     int64
	lock    sync.RWMutex
}

func NewMockDataItem(uid int64, data []byte) *MockDataItem {
	oldData := make([]byte, len(data))
	copy(oldData, data)
	mockDataItem := &MockDataItem{
		data:    data,
		oldData: oldData,
		uid:     uid,
	}
	return mockDataItem
}

func (mockDataItem *MockDataItem) Data() []byte {
	return mockDataItem.data
}

func (mockDataItem *MockDataItem) Before() {
	mockDataItem.lock.Lock()
	mockDataItem.oldData = make([]byte, len(mockDataItem.data))
	copy(mockDataItem.oldData, mockDataItem.data)
}

func (mockDataItem *MockDataItem) UnBefore() {
	mockDataItem.data = make([]byte, len(mockDataItem.oldData))
	copy(mockDataItem.data, mockDataItem.oldData)
	mockDataItem.lock.Unlock()
}

func (mockDataItem *MockDataItem) After(xid int64) {
	mockDataItem.lock.Unlock()
}

func (mockDataItem *MockDataItem) Release() {

}

func (mockDataItem *MockDataItem) Lock() {
	mockDataItem.lock.Lock()
}

func (mockDataItem *MockDataItem) UnLock() {
	mockDataItem.lock.Unlock()
}

func (mockDataItem *MockDataItem) RLock() {
	mockDataItem.lock.RLock()
}

func (mockDataItem *MockDataItem) RUnLock() {
	mockDataItem.lock.RUnlock()
}

func (mockDataItem *MockDataItem) Page() *dmPage.Page {
	return nil
}

func (mockDataItem *MockDataItem) GetUid() int64 {
	return mockDataItem.uid
}

func (mockDataItem *MockDataItem) GetOldRaw() []byte {
	return mockDataItem.oldData
}

func (mockDataItem *MockDataItem) GetRaw() []byte {
	return mockDataItem.data
}
