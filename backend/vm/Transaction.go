package vm

import "SimpleDB/backend/tm"

// Transaction 对一个事务的抽象
type Transaction struct {
	Xid int64
	// 事务的隔离级别
	Level int32
	// 事务的快照，用于存储活跃事务的ID
	SnapShot map[int64]bool
	Err      error
	// 标志事务是否自动中止
	AutoAborted bool
}

// NewTransaction 创建一个新的事务
func NewTransaction(xid int64, level int32, active map[int64]*Transaction) *Transaction {
	transaction := &Transaction{
		// 设置事务ID
		Xid: xid,
		// 设置事务隔离级别
		Level: level,
	}
	var snapShot map[int64]bool
	// 如果隔离级别不为0，创建快照
	if level != 0 {
		snapShot = make(map[int64]bool)
		// 将活跃事务的ID添加到快照中
		for k, _ := range active {
			snapShot[k] = true
		}
		transaction.SnapShot = snapShot
	}
	return transaction
}

func (t *Transaction) IsInSnapShot(xid int64) bool {
	if xid == tm.SUPER_XID {
		return false
	}
	isIn, ok := t.SnapShot[xid]
	if !ok {
		return false
	}
	return isIn
}
