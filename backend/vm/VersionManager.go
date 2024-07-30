package vm

import (
	"dbofmine/backend/common"
	"dbofmine/backend/dm"
	"dbofmine/backend/tm"
	"dbofmine/commons"
	"errors"
	"sync"
)

type VersionManager struct {
	TM                *tm.TransactionManagerImpl
	DM                *dm.DataManager
	ActiveTransaction map[int64]*Transaction

	Lock commons.ReentrantLock
	LT   *LockTable

	CacheManager *common.AbstractCache[*Entry]
}

// ================= 实例方法 =================

// Read 读取一个entry，需要判断可见性
func (versionManager *VersionManager) Read(xid int64, uid int64) ([]byte, error) {
	versionManager.Lock.Lock()
	// 从活动事务中获取事务对象
	transaction := versionManager.ActiveTransaction[xid]
	versionManager.Lock.Unlock()

	// 如果事务已经出错，那么抛出错误
	if transaction.Err != nil {
		return nil, transaction.Err
	}

	var entry *Entry = nil
	entry, err := versionManager.CacheManager.Get(uid)
	if err != nil {
		if entry != nil {
			entry.Release()
		}
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}

	// 释放数据项
	defer entry.Release()
	// 如果数据项对当前事务可见，那么返回数据项的数据
	if IsVisible(versionManager.TM, transaction, entry) {
		return entry.Data(), nil
	} else {
		return nil, nil
	}
}

// Insert 将数据包裹成Entry，然后交给DM插入即可
func (versionManager *VersionManager) Insert(xid int64, data []byte) (int64, error) {
	versionManager.Lock.Lock()
	// 从活动事务中获取事务对象
	transaction := versionManager.ActiveTransaction[xid]
	versionManager.Lock.Unlock()

	// 如果事务已经出错，那么抛出错误
	if transaction.Err != nil {
		return -1, transaction.Err
	}
	// 将事务ID和数据包装成一个新的数据项
	raw := WrapEntryRaw(xid, data)
	// 调用数据管理器的insert方法，插入新的数据项，并返回数据项的唯一标识符
	return versionManager.DM.Insert(xid, raw)
}

// Delete 删除一个数据项
func (versionManager *VersionManager) Delete(xid int64, uid int64) (bool, error) {
	versionManager.Lock.Lock()
	// 从活动事务中获取事务对象
	transaction := versionManager.ActiveTransaction[xid]
	versionManager.Lock.Unlock()

	// 如果事务已经出错，那么抛出错误
	if transaction.Err != nil {
		return false, transaction.Err
	}

	var entry *Entry = nil
	entry, err := versionManager.CacheManager.Get(uid)
	if err != nil {
		if entry != nil {
			entry.Release()
		}
		return false, err
	}
	// 如果数据项不存在，那么返回false
	if entry == nil {
		return false, nil
	}

	defer entry.Release()
	// 如果数据项对当前事务不可见，那么返回false
	if !IsVisible(versionManager.TM, transaction, entry) {
		return false, nil
	}

	var l sync.Locker = nil
	// 尝试为数据项添加锁
	l, err = versionManager.LT.Add(xid, uid)
	// 如果出现并发更新的错误，那么中止事务，并抛出错误
	if err != nil {
		transaction.Err = errors.New(commons.ErrorMessage.ConcurrentUpdateError)
		versionManager.internAbort(xid, true)
		transaction.AutoAborted = true
		return false, transaction.Err
	}
	// 如果成功获取到锁，那么锁定并立即解锁
	if l != nil {
		l.Lock()
		l.Unlock()
	}
	// 如果数据项已经被当前事务删除，那么返回false
	if entry.GetXMax() == xid {
		return false, nil
	}
	// 如果数据项的版本被跳过，那么中止事务，并抛出错误
	if IsVersionSkip(versionManager.TM, transaction, entry) {
		transaction.Err = errors.New(commons.ErrorMessage.ConcurrentUpdateError)
		versionManager.internAbort(xid, true)
		transaction.AutoAborted = true
		return false, transaction.Err
	}

	// 设置数据项的xmax为当前事务的ID，表示数据项被当前事务删除
	entry.SetXMax(xid)
	return true, nil
}

// Begin 开启一个事务，并初始化事务的结构
func (versionManager *VersionManager) Begin(level int32) int64 {
	versionManager.Lock.Lock()
	defer versionManager.Lock.Unlock()
	// 调用事务管理器的begin方法，开始一个新的事务，并获取事务ID
	xid := versionManager.TM.Begin()
	// 创建一个新的事务对象
	transaction := NewTransaction(xid, level, versionManager.ActiveTransaction)
	// 将事务对象添加到活动事务中
	versionManager.ActiveTransaction[xid] = transaction

	return xid
}

// Commit 公开的commit方法，用于提交一个事务
func (versionManager *VersionManager) Commit(xid int64) error {
	versionManager.Lock.Lock()
	// 从活动事务中获取事务对象
	transaction := versionManager.ActiveTransaction[xid]
	versionManager.Lock.Unlock()

	// 如果事务已经出错，那么抛出错误
	if transaction.Err != nil {
		commons.Logger.Errorf("事务出错：%v", transaction.Err)
		commons.Logger.Errorf("活动事务集：%v", transaction.SnapShot)
		return transaction.Err
	}

	versionManager.Lock.Lock()
	// 从活动事务中移除这个事务
	delete(versionManager.ActiveTransaction, xid)

	versionManager.Lock.Unlock()

	// 从锁表中移除这个事务的锁
	versionManager.LT.Remove(xid)
	// 调用事务管理器的commit方法，进行事务的提交操作
	versionManager.TM.Commit(xid)
	return nil
}

// Abort 公开的abort方法，用于中止一个事务，手动停止
func (versionManager *VersionManager) Abort(xid int64) {
	// 调用内部的abort方法，autoAborted参数为false表示这不是一个自动中止的事务
	versionManager.internAbort(xid, false)
}

// abort事务的方法有两种:手动和自动
// 手动指的是调用abort()方法
// 自动是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚
// internAbort 内部的abort方法，处理事务的中止
func (versionManager *VersionManager) internAbort(xid int64, autoAborted bool) {
	versionManager.Lock.Lock()
	// 从活动事务中获取事务对象
	transaction, ok := versionManager.ActiveTransaction[xid]
	if !ok {
		panic("事务不存在")
	}
	// 如果这不是一个自动中止的事务，那么从活动事务中移除这个事务
	if !autoAborted {
		delete(versionManager.ActiveTransaction, xid)
	}
	versionManager.Lock.Unlock()
	// 如果事务已经被自动中止，那么直接返回，不做任何处理
	if transaction.AutoAborted {
		return
	}
	// 从锁表中移除这个事务的锁
	versionManager.LT.Remove(xid)
	// 调用事务管理器的abort方法，进行事务的中止操作
	versionManager.TM.Abort(xid)
}

func (versionManager *VersionManager) ReleaseEntry(entry *Entry) {
	versionManager.CacheManager.Release(entry.GetUid())
}

func (versionManager *VersionManager) GetForCache(uid int64) (*Entry, error) {
	entry := LoadEntry(versionManager, uid)
	if entry == nil {
		return nil, errors.New(commons.ErrorMessage.NullEntryError)
	}

	return entry, nil
}

func (versionManager *VersionManager) ReleaseForCache(entry *Entry) {
	entry.Remove()
}

// ================= 静态方法 =================

// NewVersionManager 创建一个新的VersionManager
func NewVersionManager(transactionManager *tm.TransactionManagerImpl, dm *dm.DataManager) *VersionManager {
	vm := &VersionManager{
		TM:                transactionManager,
		DM:                dm,
		ActiveTransaction: make(map[int64]*Transaction),
		LT:                NewLockTable(),
	}
	vm.ActiveTransaction[tm.SUPER_XID] = NewTransaction(tm.SUPER_XID, 0, nil)
	cacheManager := common.NewAbstractCache[*Entry](0, vm)
	vm.CacheManager = cacheManager
	return vm
}
