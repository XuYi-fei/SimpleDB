package tests

import (
	"dbofmine/backend/utils"
	"dbofmine/commons"
	"math"
)

type MockDataManager struct {
	cache map[int64]*MockDataItem
	lock  commons.ReentrantLock
}

func NewMockDataManager() *MockDataManager {
	dm := &MockDataManager{
		cache: make(map[int64]*MockDataItem),
	}
	return dm
}

func (dm *MockDataManager) Read(uid int64) *MockDataItem {
	dm.lock.Lock()
	defer dm.lock.Unlock()

	return dm.cache[uid]
}

func (dm *MockDataManager) Insert(xid int64, data []byte) int64 {
	dm.lock.Lock()
	defer dm.lock.Unlock()

	var uid int64 = 0
	for {
		uid = int64(math.Abs(float64(utils.SafeRandomInt(math.MaxInt32))))
		if uid == 0 {
			continue
		}
		if _, ok := dm.cache[uid]; ok {
			continue
		}
		break
	}
	mockDataItem := NewMockDataItem(uid, data)
	dm.cache[uid] = mockDataItem
	return uid
}

func (dm *MockDataManager) Close() {

}
