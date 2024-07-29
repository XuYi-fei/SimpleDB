package tm

import (
	"dbofmine/commons"
	"sync"
)

type MockTransactionManager struct {
	TransactionMap *commons.SyncMap[int64, byte]
	Lock           sync.Mutex
}

func (m *MockTransactionManager) begin() int64 {
	return 0
}

func (m *MockTransactionManager) Commit(xid int64) {}

func (m *MockTransactionManager) Abort(xid int64) {}

func (m *MockTransactionManager) IsActive(xid int64) bool {
	return false
}

func (m *MockTransactionManager) IsCommitted(xid int64) bool {
	return false
}

func (m *MockTransactionManager) IsAborted(xid int64) bool {
	return false
}

func (m *MockTransactionManager) close() {}
