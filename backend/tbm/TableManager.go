package tbm

import (
	"SimpleDB/backend/dm"
	"SimpleDB/backend/parser/statement"
	"SimpleDB/backend/vm"
	"SimpleDB/commons"
	"encoding/binary"
	"errors"
	"strconv"
)

type TableManager struct {
	// 版本管理器，用于管理事务的版本
	VM *vm.VersionManager
	// 数据管理器，用于管理数据的存储和读取
	DM *dm.DataManager
	// 启动信息管理器，用于管理数据库启动信息
	booter *Booter
	// 表缓存，用于缓存已加载的表，键是表名，值是表对象
	tableCache map[string]*Table
	// 事务表缓存，用于缓存每个事务修改过的表，键是事务ID，值是表对象列表
	xidTableCache map[int64][]*Table
	lock          commons.ReentrantLock
}

func CreateTableManger(path string, vm *vm.VersionManager, dm *dm.DataManager) *TableManager {
	booter := CreateBooter(path)
	booter.Update([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	return NewTableManager(vm, dm, booter)
}

func OpenTableManager(path string, vm *vm.VersionManager, dm *dm.DataManager) *TableManager {
	booter := OpenBooter(path)
	return NewTableManager(vm, dm, booter)
}

func NewTableManager(vm *vm.VersionManager, dm *dm.DataManager, booter *Booter) *TableManager {
	tableManager := &TableManager{
		VM:            vm,
		DM:            dm,
		booter:        booter,
		tableCache:    make(map[string]*Table),
		xidTableCache: make(map[int64][]*Table),
	}

	tableManager.loadTables()
	return tableManager
}

// loadTables 加载所有的数据库表
func (tableManager *TableManager) loadTables() {
	// 获取第一个表的UID
	uid := tableManager.firstTableUid()
	// 当UID不为0时，表示还有表需要加载
	for uid != 0 {
		// 加载表，并获取表的UID
		tb := LoadTable(tableManager, uid)
		// 更新UID为下一个表的UID
		uid = tb.NextUid
		// 将加载的表添加到表缓存中
		tableManager.tableCache[tb.Name] = tb
	}
}

// firstTableUid 获取 Booter 文件的前八位字节
func (tableManager *TableManager) firstTableUid() int64 {
	raw := tableManager.booter.Load()
	uid := binary.BigEndian.Uint64(raw[:8])
	return int64(uid)
}

// updateFirstTableUid 更新 Booter 文件的前八位字节
func (tableManager *TableManager) updateFirstTableUid(uid int64) {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(uid))
	tableManager.booter.Update(raw)
}

type BeginResult struct {
	Xid    int64
	Result []byte
}

func (tableManager *TableManager) Begin(begin *statement.BeginStatement) *BeginResult {
	result := &BeginResult{}
	var level int32
	if begin.IsRepeatableRead {
		level = 1
	} else {
		level = 0
	}
	result.Xid = tableManager.VM.Begin(level)
	result.Result = []byte("begin")

	return result
}

func (tableManager *TableManager) Commit(xid int64) ([]byte, error) {
	err := tableManager.VM.Commit(xid)
	if err != nil {
		return nil, err
	}
	return []byte("commit"), nil
}

func (tableManager *TableManager) Abort(xid int64) []byte {
	tableManager.VM.Abort(xid)
	return []byte("abort")
}

func (tableManager *TableManager) Show(xid int64) []byte {
	tableManager.lock.Lock()
	defer tableManager.lock.Unlock()

	str := ""
	for _, tb := range tableManager.tableCache {
		str += tb.String()
		str += "\n"
	}

	//tables := tableManager.xidTableCache[xid]
	//if tables == nil {
	//	return []byte("No tables\n")
	//}

	//for _, tb := range tables {
	//	str += tb.String()
	//	str += "\n"
	//}
	return []byte(str)
}

func (tableManager *TableManager) Create(xid int64, create *statement.CreateStatement) ([]byte, error) {
	tableManager.lock.Lock()
	defer tableManager.lock.Unlock()

	_, ok := tableManager.tableCache[create.TableName]
	// 如果表已经存在，则返回错误
	if ok {
		return nil, errors.New(commons.ErrorMessage.DuplicatedTableError)
	}

	// 创建表
	table, err := CreateTable(tableManager, tableManager.firstTableUid(), xid, create)
	if err != nil {
		return nil, err
	}

	// 更新第一个数据表的Uid
	tableManager.updateFirstTableUid(table.Uid)

	// 将表添加到表缓存中
	tableManager.tableCache[table.Name] = table
	// 将表添加到事务表缓存中
	tableManager.xidTableCache[xid] = append(tableManager.xidTableCache[xid], table)

	return []byte("create " + create.TableName), nil
}

func (tableManager *TableManager) Insert(xid int64, insert *statement.InsertStatement) ([]byte, error) {
	tableManager.lock.Lock()

	table := tableManager.tableCache[insert.TableName]

	tableManager.lock.Unlock()

	if table == nil {
		return nil, errors.New(commons.ErrorMessage.TableNotFoundError)
	}
	err := table.Insert(xid, insert)
	if err != nil {
		return nil, err
	}
	return []byte("insert "), nil
}

func (tableManager *TableManager) Read(xid int64, read *statement.SelectStatement) ([]byte, error) {
	tableManager.lock.Lock()
	table := tableManager.tableCache[read.TableName]
	tableManager.lock.Unlock()

	if table == nil {
		return nil, errors.New(commons.ErrorMessage.TableNotFoundError)
	}

	data, err := table.Read(xid, read)
	if err != nil {
		return nil, err
	}

	return []byte(data), nil
}

func (tableManager *TableManager) Update(xid int64, update *statement.UpdateStatement) ([]byte, error) {
	tableManager.lock.Lock()
	table := tableManager.tableCache[update.TableName]
	tableManager.lock.Unlock()

	if table == nil {
		return nil, errors.New(commons.ErrorMessage.TableNotFoundError)
	}
	count, err := table.Update(xid, update)
	if err != nil {
		return nil, err
	}
	return []byte("update " + strconv.Itoa(count)), nil
}

func (tableManager *TableManager) Delete(xid int64, deleteStatement *statement.DeleteStatement) ([]byte, error) {
	tableManager.lock.Lock()
	table := tableManager.tableCache[deleteStatement.TableName]
	tableManager.lock.Unlock()

	if table == nil {
		return nil, errors.New(commons.ErrorMessage.TableNotFoundError)
	}

	count, err := table.Delete(xid, deleteStatement)
	if err != nil {
		return nil, err
	}
	return []byte("delete " + strconv.Itoa(count)), nil
}
