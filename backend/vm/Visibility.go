package vm

import "SimpleDB/backend/tm"

// IsVersionSkip 判断是否发生了MVCC中的版本跳跃问题
func IsVersionSkip(tm *tm.TransactionManagerImpl, t *Transaction, e *Entry) bool {
	XMax := e.GetXMax()
	if t.Level == 0 {
		return false
	} else {
		return tm.IsCommitted(XMax) && (XMax > t.Xid || t.IsInSnapShot(XMax))
	}
}

// IsVisible 判断记录e是否对事务t可见
func IsVisible(tm *tm.TransactionManagerImpl, t *Transaction, e *Entry) bool {
	// 如果是可重复读级别
	if t.Level == 0 {
		return readCommitted(tm, t, e)
	} else {
		return repeatableRead(tm, t, e)
	}
}

// readCommitted 如果是读已提交的隔离级别，判断e是否对事务t可见
func readCommitted(tm *tm.TransactionManagerImpl, t *Transaction, e *Entry) bool {
	// 获取事务的ID
	xid := t.Xid
	// 获取记录的创建版本号
	XMin := e.GetXMin()
	// 获取记录的删除版本号
	XMax := e.GetXMax()
	// 如果记录的创建版本号等于事务的ID并且记录未被删除，则返回true
	// ---即记录e由当前事务创建且还未被删除
	if XMin == xid && XMax == 0 {
		return true
	}

	// 如果记录的创建版本已经提交
	if tm.IsCommitted(XMin) {
		// 如果记录未被删除，则返回true
		if XMax == 0 {
			// ---这里代表的其实就是e由一个已提交的事务创建并且还未被删除
			return true
		}

		// 如果记录的删除版本号不等于事务的ID
		if XMax != xid {
			// 如果记录的删除版本未提交，则返回true
			// 因为没有提交，代表该数据还是上一个版本可见的
			if !tm.IsCommitted(XMax) {
				// ---这里代表的是e由一个未提交的事务删除
				return true
			}
		}
	}
	return false
}

// repeatableRead 如果是可重复读的隔离级别，判断e是否对事务t可见
func repeatableRead(tm *tm.TransactionManagerImpl, t *Transaction, e *Entry) bool {
	// 获取事务的ID
	xid := t.Xid
	// 获取记录的创建版本号
	XMin := e.GetXMin()
	// 获取记录的删除版本号
	XMax := e.GetXMax()
	// 如果记录的创建版本号等于事务的ID并且记录未被删除，则返回true
	// ---即记录e由当前事务创建且还未被删除
	if XMin == xid && XMax == 0 {
		return true
	}
	// 如果记录e的创建版本已经提交，并且创建版本号小于事务的ID，并且创建版本号不在事务的快照中
	if tm.IsCommitted(XMin) && XMin < xid && !t.IsInSnapShot(XMin) {
		// 如果条目未被删除，则返回true
		if XMax == 0 {
			return true
		}
		// 如果条目的删除版本号不等于事务的ID
		if XMin != xid {
			// 如果条目的删除版本未提交，或者删除版本号大于事务的ID，或者删除版本号在事务的快照中，则返回true
			if !tm.IsCommitted(XMax) || XMax > xid || t.IsInSnapShot(XMax) {
				return true
			}
		}
	}
	return false
}
