package dm

import (
	"dbofmine/backend/common"
	"dbofmine/backend/dm/dmPage"
	"dbofmine/backend/dm/dmPageIndex"
	"dbofmine/backend/dm/logger"
	"dbofmine/backend/tm"
	"dbofmine/backend/utils"
	"dbofmine/commons"
	"errors"
)

type DataManager struct {
	TM       *tm.TransactionManagerImpl
	PC       *dmPage.PageCache
	DBLogger *logger.DBLogger
	PIndex   *dmPageIndex.PageIndex
	PageOne  *dmPage.Page
	// 抽象缓存类
	CacheManager *common.AbstractCache[*DataItem]
}

func NewDataManager(tm *tm.TransactionManagerImpl, pc *dmPage.PageCache, dbLogger *logger.DBLogger) *DataManager {

	dataManager := &DataManager{
		TM:       tm,
		PC:       pc,
		DBLogger: dbLogger,
		PIndex:   dmPageIndex.NewPageIndex(),
	}

	// 实现类似抽象类的实现作用
	cacheManager := common.NewAbstractCache[*DataItem](0, dataManager)

	dataManager.CacheManager = cacheManager

	return dataManager
}

// Read 读取数据
func (dataManager *DataManager) Read(uid int64) *DataItem {
	dataItem, _ := dataManager.CacheManager.Get(uid)
	if !dataItem.IsValid() {
		dataItem.Release()
		return nil
	}
	return dataItem
}

// Insert 插入数据
func (dataManager *DataManager) Insert(xid int64, data []byte) (int64, error) {
	// 将原始数据封装成DataItem格式
	raw := WrapDataItemRaw(data)
	// 数据都大于了页面的理论最大空间，报错；这里注意数据大小不能大于一个页面大小，即8K减去前面元信息
	if len(raw) > dmPage.PageXMaxFreeSpace {
		return 0, errors.New(commons.ErrorMessage.DataTooLargeError)
	}

	// 从页面的索引信息中获取一个仍有足够空闲的页面
	var pageInfo *dmPageIndex.PageInfo
	// 循环多取几次，一次取不到就创建一个新页面，这样操作5次还是取不到就算了
	for i := 0; i < 5; i++ {
		pageInfo = dataManager.PIndex.Select(int32(len(raw)))
		if pageInfo != nil {
			//commons.Logger.Debugf("Existing page: %d", pageInfo.PageNumber)
			break
		} else {
			newPageNumber := dataManager.PC.NewPage(dmPage.PageXInitRaw())
			//commons.Logger.Debugf("NewPage: %d", newPageNumber)
			dataManager.PIndex.Add(int32(newPageNumber), int32(dmPage.PageXMaxFreeSpace))
		}
	}
	//commons.Logger.Debugf("PC's CacheManager: %v", dataManager.PC.CacheManager)
	if pageInfo == nil {
		return 0, errors.New(commons.ErrorMessage.DatabaseBusyError)
	}

	// 取出索引的页面信息后获取仍有空闲的页面
	var page *dmPage.Page
	// freeSpace表示该页仍然空闲的大小
	var freeSpace int32 = 0

	// 如果出错了，那么更新索引信息
	defer func() {
		if page != nil {
			dataManager.PIndex.Add(pageInfo.PageNumber, dmPage.PageXGetFreeSpace(page))
		} else {
			dataManager.PIndex.Add(pageInfo.PageNumber, freeSpace)
		}
	}()

	page, err := dataManager.PC.GetPage(int(pageInfo.PageNumber))
	if err != nil {
		panic(err)
	}
	insertLog := InsertLog(xid, page, raw)
	// 将日志写入日志文件
	dataManager.DBLogger.Log(insertLog)

	offset := dmPage.InsertData2PageX(page, raw)
	page.Release()
	return utils.GenerateUID(int(pageInfo.PageNumber), int(offset)), nil
}

// Close 关闭数据管理器
func (dataManager *DataManager) Close() {
	dataManager.CacheManager.Close()
	dataManager.DBLogger.Close()

	dmPage.PageOneSetValidStatusClose(dataManager.PageOne)
	dataManager.PageOne.Release()
	dataManager.PC.Close()
}

// LogDataItem 为xid生成update日志
func (dataManager *DataManager) LogDataItem(xid int64, dataItem *DataItem) {
	log := UpdateLog(xid, dataItem)
	dataManager.DBLogger.Log(log)
}

func (dataManager *DataManager) ReleaseDataItem(dataItem *DataItem) {
	dataManager.CacheManager.Release(dataItem.UID())
}

// InitPageOne 在创建文件时初始化第一页
func (dataManager *DataManager) InitPageOne() {
	pageOne := dataManager.PC.NewPage(dmPage.PageOneInitRaw())
	if pageOne != 1 {
		panic(errors.New("page one is not 1"))
	}
	page, err := dataManager.PC.GetPage(pageOne)
	if err != nil {
		panic(err)
	}
	dataManager.PageOne = page
	dataManager.PC.FlushPage(dataManager.PageOne)
}

// LoadCheckPageOne 在打开已有文件时时读入PageOne，并验证正确性
func (dataManager *DataManager) LoadCheckPageOne() bool {
	page, err := dataManager.PC.GetPage(1)
	dataManager.PageOne = page
	if err != nil {
		panic(err)
	}
	return dmPage.CheckPageOneValid(page)
}

// FillPageIndex 初始化pageIndex
func (dataManager *DataManager) FillPageIndex() {
	pageNumber := dataManager.PC.GetPageNumber()
	for i := 2; i <= pageNumber; i++ {
		page, err := dataManager.PC.GetPage(i)
		if err != nil {
			panic(err)
		}
		freeSpace := dmPage.PageXGetFreeSpace(page)
		dataManager.PIndex.Add(int32(page.GetPageNumber()), freeSpace)
		page.Release()
	}
}

func (dataManager *DataManager) GetForCache(uid int64) (*DataItem, error) {
	offset := int32(uid & ((1 << 16) - 1))
	uid >>= 32
	pageNumber := int(uid & ((1 << 32) - 1))
	page, err := dataManager.PC.GetPage(pageNumber)
	if err != nil {
		return nil, err
	}
	return ParseDataItem(page, offset, dataManager), nil
}

func (dataManager *DataManager) ReleaseForCache(dataItem *DataItem) {
	dataItem.Page().Release()
}

func CreateDataManager(path string, memory int64, tm *tm.TransactionManagerImpl) *DataManager {
	PC := dmPage.CreatePageCacheImpl(path, memory)
	DBLogger := logger.CreateLogger(path)
	dataManager := NewDataManager(tm, PC, DBLogger)
	dataManager.InitPageOne()

	return dataManager
}

//func CreateDataManagerByMockTM(path string, memory int64, tm *tm.MockTransactionManager) *DataManager {
//	PC := dmPage.CreatePageCacheImpl(path, memory)
//	DBLogger := logger.CreateLogger(path)
//	dataManager := NewDataManager(tm, PC, DBLogger)
//	dataManager.InitPageOne()
//
//	return dataManager
//}

func OpenDataManager(path string, memory int64, tm *tm.TransactionManagerImpl) *DataManager {
	PC := dmPage.OpenPageCacheImpl(path, memory)
	DBLogger := logger.OpenLogger(path)
	dataManager := NewDataManager(tm, PC, DBLogger)
	if !dataManager.LoadCheckPageOne() {
		Recover(tm, DBLogger, PC)
	}
	dataManager.FillPageIndex()
	dmPage.PageOneSetValidStatusOpen(dataManager.PageOne)
	dataManager.PC.FlushPage(dataManager.PageOne)

	return dataManager
}
