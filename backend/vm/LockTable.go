package vm

import (
	"dbofmine/commons"
	"errors"
	"sync"
)

// LockTable 维护了一个依赖等待图，以进行死锁检测
type LockTable struct {
	// 某个XID已经获得的资源的UID列表，键是事务ID，值是该事务持有的资源ID列表。
	x2u map[int64][]int64
	// UID被某个XID持有,键是资源ID，值是持有该资源的事务ID。
	u2x map[int64]int64
	// 正在等待UID的XID列表，键是资源ID，值是正在等待该资源的事务ID。
	wait map[int64][]int64
	// 正在等待资源的XID的锁,键是事务ID，值是该事务的锁对象。
	waitLock map[int64]sync.Locker
	// XID正在等待的UID,键是事务ID，值是该事务正在等待的资源ID。
	waitU map[int64]int64
	lock  commons.ReentrantLock

	// 以下两个字段用于死锁检测
	// xidStamp 事务ID的时间戳映射
	xidStamp map[int64]int
	// stamp 全局的时间戳
	stamp int
}

func NewLockTable() *LockTable {
	return &LockTable{
		x2u:      make(map[int64][]int64),
		u2x:      make(map[int64]int64),
		wait:     make(map[int64][]int64),
		waitLock: make(map[int64]sync.Locker),
		waitU:    make(map[int64]int64),
	}
}

// Add 添加一个事务ID和资源ID的映射关系，返回一个锁对象，如果发生死锁，返回错误
func (lockTable *LockTable) Add(xid int64, uid int64) (sync.Locker, error) {
	lockTable.lock.Lock()
	defer lockTable.lock.Unlock()

	// 检查x2u是否已经拥有这个资源
	// 如果事务xid已经持有资源uid，返回nil
	if lockTable.isInList(xid, uid) {
		// 如果已经拥有，直接返回nil
		return nil, nil
	}
	// 检查UID资源是否已经被其他XID事务持有
	if _, ok := lockTable.u2x[uid]; !ok {
		// 如果没有被持有，将资源分配给当前事务
		lockTable.u2x[uid] = xid
		// 将资源添加到事务的资源列表中
		lockTable.x2u[xid] = append(lockTable.x2u[xid], uid)
		// 返回nil
		return nil, nil
	}

	// 如果资源已经被其他事务持有，将当前事务添加到等待列表中(waitU是一个map，键是事务ID，值是资源ID，代表事务正在等待的资源)
	lockTable.waitU[xid] = uid
	// 反过来，将资源添加到等待列表中(wait是一个map，键是资源ID，值是事务ID列表，代表正在等待该资源的事务)
	lockTable.wait[uid] = append(lockTable.wait[uid], xid)

	// 检查是否存在死锁
	if lockTable.hasDeadLock() {
		// 如果存在死锁，从等待列表中移除当前事务
		delete(lockTable.waitU, xid)
		// 从资源的等待列表中移除当前事务
		lockTable.removeFromList(lockTable.wait, uid, xid)
		// 返回错误
		return nil, errors.New(commons.ErrorMessage.DeadLockError)
	}

	// 如果不存在死锁，为当前事务创建一个新的锁，并锁定它
	lock := &commons.ReentrantLock{}
	lock.Lock()
	lockTable.waitLock[xid] = lock
	return lock, nil
}

// Remove 当一个事务commit或者abort时，就会释放掉它自己持有的锁，并将自身从等待图中删除
func (lockTable *LockTable) Remove(xid int64) {
	lockTable.lock.Lock()
	defer lockTable.lock.Unlock()

	// 从x2u映射中获取当前事务ID已经获得的资源的UID列表
	uids, ok := lockTable.x2u[xid]
	if ok {
		for len(uids) > 0 {
			// 获取并移除列表中的第一个资源ID
			uid := uids[0]
			uids = uids[1:]
			// 从等待队列中选择一个新的事务ID来占用这个资源
			lockTable.selectNewXID(uid)
		}
	}
	// 从waitU映射中移除当前事务ID
	delete(lockTable.waitU, xid)
	// 从x2u映射中移除当前事务ID
	delete(lockTable.x2u, xid)
	// 从waitLock映射中移除当前事务ID
	delete(lockTable.waitLock, xid)
}

// 从等待队列中选择一个xid来占用uid
func (lockTable *LockTable) selectNewXID(uid int64) {
	// 从u2x映射中移除当前资源ID
	delete(lockTable.u2x, uid)
	// 从等待队列中获取当前资源ID的等待列表
	xids, ok := lockTable.wait[uid]
	// 如果等待队列为空，立即返回
	if !ok {
		return
	}
	// 断言等待队列不为空
	if len(xids) == 0 {
		panic("xids 理论不该为0！")
	}

	// 遍历等待队列
	for len(xids) > 0 {
		// 获取并移除队列中的第一个事务ID
		xid := xids[0]
		xids = xids[1:]
		// 检查事务ID是否在waitLock映射中
		lock, ok := lockTable.waitLock[xid]
		// 如果在waitLock映射中，表示这个事务ID已经被锁定
		if !ok {
			continue
		} else {
			// 将事务ID和资源ID添加到u2x映射中
			lockTable.u2x[uid] = xid
			// 从waitLock映射中移除这个事务ID
			delete(lockTable.waitLock, xid)
			// 从waitU映射中移除这个事务ID
			delete(lockTable.waitU, xid)
			// 解锁这个事务ID的锁
			lock.Unlock()
			break
		}
	}
}

// isInList 给定事务xid和资源uid，判断当前事务是否持有该资源，如果已经持有，返回true，否则返回false
func (lockTable *LockTable) isInList(xid int64, uid int64) bool {
	// 先获取事务xid持有的资源列表
	uids, ok := lockTable.x2u[xid]
	if !ok {
		return false
	}
	// 遍历资源列表，如果找到了资源uid，返回true
	for _, u := range uids {
		if u == uid {
			return true
		}
	}
	return false
}

func (lockTable *LockTable) removeFromList(listMap map[int64][]int64, uid0 int64, uid1 int64) {
	l, ok := listMap[uid0]
	if !ok {
		return
	}
	for i, u := range l {
		if u == uid1 {
			l = append(l[:i], l[i+1:]...)
			listMap[uid0] = l
			break
		}
	}
	if len(l) == 0 {
		delete(listMap, uid0)
	}
}

// hasDeadLock 检查是否存在死锁
func (lockTable *LockTable) hasDeadLock() bool {
	// TODO：这里需不需要加锁呢？
	lockTable.lock.Lock()
	defer lockTable.lock.Unlock()

	// 创建一个新的xidStamp哈希映射
	lockTable.xidStamp = make(map[int64]int)
	// 将stamp设置为1
	lockTable.stamp = 1
	// 遍历所有已经获得资源的事务ID
	for xid, _ := range lockTable.x2u {
		// 获取xidStamp中对应事务ID的记录
		s, ok := lockTable.xidStamp[xid]
		// 如果记录存在，并且值大于0
		if ok && s > 0 {
			// 跳过这个事务ID，继续下一个
			continue
		}
		// 将stamp加1
		lockTable.stamp++
		// 调用dfs方法进行深度优先搜索
		if lockTable.dfs(xid) {
			// 如果dfs方法返回true，表示存在死锁，那么hasDeadLock方法也返回true
			return true
		}

	}
	// 如果所有的事务ID都被检查过，并且没有发现死锁，那么hasDeadLock方法返回false
	return false
}

// dfs 深度优先搜索，检查是否存在死锁
func (lockTable *LockTable) dfs(xid int64) bool {
	// 从xidStamp映射中获取当前事务ID的时间戳
	stamp, ok := lockTable.xidStamp[xid]
	// 如果时间戳存在并且等于全局时间戳，说明发生了死锁（这意味着是在dfs递归的过程中又检查到了这个xid）
	if ok && stamp == lockTable.stamp {
		// 存在死锁，返回true
		return true
	}
	// 如果时间戳存在并且小于全局时间戳
	if ok && stamp < lockTable.stamp {
		// 这个事务ID已经被检查过，并且没有发现死锁，返回false
		return false
	}

	// 如果时间戳不存在，将当前事务ID的时间戳设置为全局时间戳
	lockTable.xidStamp[xid] = lockTable.stamp

	// 从waitU映射中获取当前事务ID正在等待的资源ID
	uid, ok := lockTable.waitU[xid]
	if !ok {
		// 如果资源ID不存在，表示当前事务ID不在等待任何资源，返回false
		return false
	}
	// 从u2x映射中获取当前资源ID被哪个事务ID持有，这里应该是一定有的，因为在Add方法中，如果资源ID存在，一定会被分配给一个事务ID
	holdXid, ok := lockTable.u2x[uid]
	if !ok {
		panic("uid should be hold by some xid")
	}
	return lockTable.dfs(holdXid)
}
