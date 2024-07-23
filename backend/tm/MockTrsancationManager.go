package tm

import (
	"dbofmine/backend/commons"
	"sync"
)

type MockTransactionManager struct {
	TransactionMap *commons.SyncMap[int64, byte]
	Lock           sync.Mutex
}

func (m *MockTransactionManager) begin() int64 {
	return 0
}

func (m *MockTransactionManager) commit(xid int64) {}

func (m *MockTransactionManager) abort(xid int64) {}

func (m *MockTransactionManager) isActive(xid int64) bool {
	return false
}

func (m *MockTransactionManager) isCommitted(xid int64) bool {
	return false
}

func (m *MockTransactionManager) isAborted(xid int64) bool {
	return false
}

func (m *MockTransactionManager) close() {}
