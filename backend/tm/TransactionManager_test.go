package tm

import (
	"SimpleDB/backend/utils"
	"SimpleDB/commons"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
)

var (
	workerNum          = 50
	workerLoop         = 1000
	transactionMap     = &commons.SyncMap[int64, byte]{}
	transactionCnt     = 0
	transactionManager *TransactionManagerImpl
	lock               sync.Mutex
)

// NewMockTransactionManager create a new TransactionManager
func NewMockTransactionManager() *MockTransactionManager {
	return &MockTransactionManager{
		TransactionMap: &commons.SyncMap[int64, byte]{},
		Lock:           sync.Mutex{},
	}
}

func TestMultiThread(t *testing.T) {
	// 类似CountDownLatch
	var wg sync.WaitGroup
	wg.Add(workerNum)

	transactionManager, _ := CreateTransactionManagerImpl("/Users/xuyifei/repos/SimpleDB/data/test/backend/tm/tranmger_test")

	defer os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/tm/tranmger_test" + XidSuffix)

	for i := 0; i < workerNum; i++ {
		go func() {
			isTrans := false
			var transactionXid int64
			for j := 0; j < workerLoop; j++ {
				op := int(math.Abs(float64(utils.SafeRandomInt(6))))
				if op == 0 {
					lock.Lock()
					// 判断是否有事务在进行中（未提交或未终止）
					if !isTrans {
						// 开启事务获得分配得到的事务id
						xid := transactionManager.Begin()
						transactionMap.Store(xid, byte(0))
						transactionCnt++
						transactionXid = xid
						isTrans = true
					} else {
						var status int = rand.Int()%2 + 1
						switch status {
						case 1:
							transactionManager.Commit(transactionXid)
							break
						case 2:
							transactionManager.Abort(transactionXid)
							break
						}
						transactionMap.Store(transactionXid, byte(status))
						isTrans = false
					}
					lock.Unlock()
				} else {
					lock.Lock()
					if transactionCnt > 0 {
						xid := int64(rand.Intn(math.MaxInt64)%transactionCnt + 1)
						status, _ := transactionMap.Load(xid)
						ok := false

						switch status {
						case 0:
							ok = transactionManager.IsActive(xid)
							break
						case 1:
							ok = transactionManager.IsCommitted(xid)
							break
						case 2:
							ok = transactionManager.IsAborted(xid)
							break
						}
						if !ok {
							t.Errorf("Transaction dealt failed!")
							return
						}
					}
					lock.Unlock()
				}
			}
			wg.Done()

		}()
	}

	wg.Wait()
	//fmt.Print(manager)
}
