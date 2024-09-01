package dm

import (
	"SimpleDB/backend/common"
	"SimpleDB/backend/dm/dmPage"
	"SimpleDB/backend/dm/dmPageIndex"
	"SimpleDB/backend/dm/logger"
	"SimpleDB/backend/tm"
	"SimpleDB/backend/utils"
	"SimpleDB/commons"
	"errors"
)

type DataManager struct {
	// TM 事务管理器
	//TM *tm.TransactionManagerImpl
	// PC 页面缓存
	PC *dmPage.PageCache
	// DBLogger 数据库日志
	DBLogger *logger.DBLogger
	// PIndex 页面索引
	PIndex *dmPageIndex.PageIndex
	// PageOne 第一页
	PageOne *dmPage.Page
	// CacheManager 抽象缓存
	CacheManager *common.AbstractCache[*DataItem]
}

func NewDataManager(pc *dmPage.PageCache, dbLogger *logger.DBLogger) *DataManager {

	dataManager := &DataManager{
		//TM:       tm,
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
	//从缓存页面中读取到DataItem
	dataItem, _ := dataManager.CacheManager.Get(uid)
	//校验di是否有效
	if !dataItem.IsValid() {
		// 无效释放缓存
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
		// 从页面索引中选择一个可以容纳新数据项的页面
		pageInfo = dataManager.PIndex.Select(int32(len(raw)))
		// 如果找到了合适的页面，跳出循环
		if pageInfo != nil {
			break
		} else {
			// 如果没有找到合适的页面，创建一个新的页面，并将其添加到页面索引中
			newPageNumber := dataManager.PC.NewPage(dmPage.PageXInitRaw())
			dataManager.PIndex.Add(int32(newPageNumber), int32(dmPage.PageXMaxFreeSpace))
		}
	}
	// 如果还是没有找到合适的页面，抛出异常
	if pageInfo == nil {
		return 0, errors.New(commons.ErrorMessage.DatabaseBusyError)
	}

	// 取出索引的页面信息后获取仍有空闲的页面
	var page *dmPage.Page
	// freeSpace表示该页仍然空闲的大小
	var freeSpace int32 = 0

	// 如果出错了，那么更新索引信息
	defer func() {
		// 将页面重新添加到页面索引中
		if page != nil {
			dataManager.PIndex.Add(pageInfo.PageNumber, dmPage.PageXGetFreeSpace(page))
		} else {
			dataManager.PIndex.Add(pageInfo.PageNumber, freeSpace)
		}
	}()

	// 获取页面信息对象中的页面
	page, err := dataManager.PC.GetPage(int(pageInfo.PageNumber))
	if err != nil {
		panic(err)
	}
	// 生成插入日志
	insertLog := InsertLog(xid, page, raw)
	// 将日志写入日志文件

	dataManager.DBLogger.Log(insertLog)
	// 在页面中插入新的数据项，并获取其在页面中的偏移量
	offset := dmPage.InsertData2PageX(page, raw)
	// 释放页面
	page.Release()
	// 返回新插入的数据项的唯一标识符，即uid
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
	// 读取第一页
	page, err := dataManager.PC.GetPage(1)
	dataManager.PageOne = page
	if err != nil {
		panic(err)
	}
	return dmPage.CheckPageOneValid(page)
}

// FillPageIndex 初始化pageIndex
func (dataManager *DataManager) FillPageIndex() {
	// 获取当前的pageCache中的页面数量
	pageNumber := dataManager.PC.GetPageNumber()
	// 遍历从第二页开始的每一页
	for i := 2; i <= pageNumber; i++ {
		// 获取第i页
		page, err := dataManager.PC.GetPage(i)
		if err != nil {
			panic(err)
		}
		// 获取第i页的空闲空间大小
		freeSpace := dmPage.PageXGetFreeSpace(page)
		// 将第i页的页面编号和空闲空间大小添加到 PageIndex 中
		dataManager.PIndex.Add(int32(page.GetPageNumber()), freeSpace)
		page.Release()
	}
}

// GetForCache 为缓存获取DataItem数据
func (dataManager *DataManager) GetForCache(uid int64) (*DataItem, error) {
	// 从 uid 中提取出偏移量（offset），这是通过位操作实现的，偏移量是 uid 的低16位，不过这里使用了 int32 类型，并不影响结果
	offset := int32(uid & ((1 << 16) - 1))
	// 将 uid 右移32位，以便接下来提取出页面编号（pageNumber）
	uid >>= 32
	// 从 uid 中提取出页面编号（pageNumber），页面编号是 uid 的高32位
	pageNumber := int(uid & ((1 << 32) - 1))
	// 使用页面缓存（PC）的 getPage(int pageNumber) 方法根据页面编号获取一个 Page 对象
	page, err := dataManager.PC.GetPage(pageNumber)
	if err != nil {
		return nil, err
	}
	// 使用 ParseDataItem 函数
	// 根据获取到的 Page 对象、偏移量和当前的 DataManager 结构体解析出一个 DataItem 对象，并返回这个对象
	return ParseDataItem(page, offset, dataManager), nil
}

// ReleaseForCache 为缓存释放DataItem数据
func (dataManager *DataManager) ReleaseForCache(dataItem *DataItem) {
	dataItem.Page().Release()
}

// CreateDataManager 创建数据管理器
func CreateDataManager(path string, memory int64) *DataManager {
	// 创建一个PageCache实例，path是文件路径，mem是内存大小
	PC := dmPage.CreatePageCache(path, memory)
	// 创建一个Logger实例，path是文件路径
	DBLogger := logger.CreateLogger(path)
	// 创建一个DataManager实例，pc是PageCache实例，lg是Logger实例，tm是TransactionManager实例
	//dataManager := NewDataManager(tm, PC, DBLogger)
	dataManager := NewDataManager(PC, DBLogger)
	// 初始化PageOne
	dataManager.InitPageOne()
	// 返回创建的DataManagerImpl实例
	return dataManager
}

//func CreateDataManagerByMockTM(path string, memory int64, tm *tm.MockTransactionManager) *DataManager {
//	PC := dmPage.CreatePageCache(path, memory)
//	DBLogger := logger.CreateLogger(path)
//	dataManager := NewDataManager(tm, PC, DBLogger)
//	dataManager.InitPageOne()
//
//	return dataManager
//}

// OpenDataManager 打开一个数据管理器
func OpenDataManager(path string, memory int64, tm *tm.TransactionManagerImpl) *DataManager {
	// 打开一个PageCache实例，path是文件路径，mem是内存大小
	PC := dmPage.OpenPageCache(path, memory)
	// 打开一个Logger实例，path是文件路径
	DBLogger := logger.OpenLogger(path)
	// 创建一个DataManager 实例，pc是PageCache实例，lg是Logger实例，tm是TransactionManager实例
	//dataManager := NewDataManager(tm, PC, DBLogger)
	dataManager := NewDataManager(PC, DBLogger)
	// 加载并检查PageOne，如果检查失败，则进行恢复操作
	if !dataManager.LoadCheckPageOne() {
		Recover(tm, DBLogger, PC)
	}
	// 填充PageIndex，遍历从第二页开始的每一页，将每一页的页面编号和空闲空间大小添加到 PageIndex 中
	dataManager.FillPageIndex()
	// 设置PageOne为打开状态
	dmPage.PageOneSetValidStatusOpen(dataManager.PageOne)
	// 将PageOne立即写入到磁盘中，确保PageOne的数据被持久化
	dataManager.PC.FlushPage(dataManager.PageOne)
	// 返回创建的DataManager实例
	return dataManager
}
