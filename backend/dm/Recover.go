package dm

import (
	"SimpleDB/backend/dm/dmPage"
	"SimpleDB/backend/dm/logger"
	"SimpleDB/backend/tm"
	"SimpleDB/commons"
	"encoding/binary"
)

var (
	LogTypeInsert byte = 0
	LogTypeUpdate byte = 1

	TypeRedo byte = 0
	TypeUndo byte = 1

	LogOffsetType = 0
	LogOffsetXID  = LogOffsetType + 1

	InsertLogOffsetPageNumber = LogOffsetXID + 8
	InsertLogOffsetOffset     = InsertLogOffsetPageNumber + 4
	InsertLogOffsetRaw        = InsertLogOffsetOffset + 2

	UpdateLogOffsetUID    = LogOffsetXID + 8
	UpdateLogOffsetOldRaw = UpdateLogOffsetUID + 8
)

// InsertLogInfo 格式 [LogType] [XID] [Pgno] [Offset] [Raw]
type InsertLogInfo struct {
	xid        int64
	pageNumber int
	offset     int16
	raw        []byte
}

func IsInsertLog(log []byte) bool {
	return log[0] == LogTypeInsert
}

// InsertLog 生成插入日志
func InsertLog(xid int64, pg *dmPage.Page, raw []byte) []byte {
	var logType []byte = []byte{LogTypeInsert}
	// xid 长度为8
	var xidBytes []byte = make([]byte, 8)
	binary.BigEndian.PutUint64(xidBytes, uint64(xid))
	// pageNumber 长度为4
	var pageNumber []byte = make([]byte, 4)
	binary.BigEndian.PutUint32(pageNumber, uint32(pg.GetPageNumber()))
	// offset 长度为2
	var offset []byte = make([]byte, 2)
	binary.BigEndian.PutUint16(offset, uint16(dmPage.PageXGetPageFreeSpaceOffset(pg)))

	return commons.BytesConcat(logType, xidBytes, pageNumber, offset, raw)
}

// parseInsertLog 解析插入日志
func parseInsertLog(log []byte) *InsertLogInfo {
	xid := int64(binary.BigEndian.Uint64(log[LogOffsetXID:InsertLogOffsetPageNumber]))
	pageNumber := int(binary.BigEndian.Uint32(log[InsertLogOffsetPageNumber:InsertLogOffsetOffset]))
	offset := int16(binary.BigEndian.Uint16(log[InsertLogOffsetOffset:InsertLogOffsetRaw]))
	raw := log[InsertLogOffsetRaw:len(log)]
	return &InsertLogInfo{
		xid:        xid,
		pageNumber: pageNumber,
		offset:     offset,
		raw:        raw,
	}
}

// doInsertLog 执行插入日志
func doInsertLog(pc *dmPage.PageCache, log []byte, logType byte) {
	insertInfoLog := parseInsertLog(log)
	page, err := pc.GetPage(insertInfoLog.pageNumber)
	if err != nil {
		panic(err)
	}
	// 如果类型的是Undo，那么需要将数据项标记为无效
	if logType == TypeUndo {
		SetDataItemRawInValid(insertInfoLog.raw)
	}
	// 将数据项插入到页面中，这里同时适用于undo和redo类型，因为上面的undo已经将数据项标记为无效了，所以这里会直接插入
	dmPage.PageXRecoverInsert(page, insertInfoLog.raw, insertInfoLog.offset)
	page.Release()
}

// UpdateLogInfo 格式 [LogType] [XID] [UID] [OldRaw] [NewRaw]
type UpdateLogInfo struct {
	xid        int64
	pageNumber int
	offset     int16
	oldRaw     []byte
	newRaw     []byte
}

// UpdateLog 生成更新日志
func UpdateLog(xid int64, di *DataItem) []byte {
	var logType []byte = []byte{LogTypeUpdate}
	// xid 长度为8
	var xidBytes []byte = make([]byte, 8)
	binary.BigEndian.PutUint64(xidBytes, uint64(xid))
	// uid 长度为8
	var uidBytes []byte = make([]byte, 8)
	binary.BigEndian.PutUint64(uidBytes, uint64(di.UID()))
	// oldRaw 长度为len(di.GetOldRaw())
	var oldRaw []byte = di.GetOldRaw()
	// newRaw 长度为len(di.GetRaw())
	newRaw := make([]byte, len(di.GetRaw()))
	copy(newRaw, di.GetRaw())

	return commons.BytesConcat(logType, xidBytes, uidBytes, oldRaw, newRaw)

}

// parseUpdateLog 解析更新日志
func parseUpdateLog(log []byte) *UpdateLogInfo {
	xid := int64(binary.BigEndian.Uint64(log[LogOffsetXID:UpdateLogOffsetUID]))
	var uid int64 = int64(binary.BigEndian.Uint64(log[UpdateLogOffsetUID:UpdateLogOffsetOldRaw]))
	offset := int16(uid & ((1 << 16) - 1))
	uid >>= 32
	pageNumber := int(uid & ((1 << 32) - 1))
	// 这里oldRaw和newRaw的数据长度应该是一样的
	length := (len(log) - UpdateLogOffsetOldRaw) / 2
	oldRaw := log[UpdateLogOffsetOldRaw : UpdateLogOffsetOldRaw+length]
	newRaw := log[UpdateLogOffsetOldRaw+length : UpdateLogOffsetOldRaw+length*2]
	return &UpdateLogInfo{
		xid:        xid,
		pageNumber: pageNumber,
		offset:     offset,
		oldRaw:     oldRaw,
		newRaw:     newRaw,
	}
}

// doUpdateLog 执行更新日志
func doUpdateLog(pc *dmPage.PageCache, log []byte, logType byte) {
	var pageNumber int32
	var offset int16
	var raw []byte
	if logType == TypeRedo {
		updateInfoLog := parseUpdateLog(log)
		pageNumber = int32(updateInfoLog.pageNumber)
		offset = updateInfoLog.offset
		raw = updateInfoLog.newRaw
	} else {
		updateInfoLog := parseUpdateLog(log)
		pageNumber = int32(updateInfoLog.pageNumber)
		offset = updateInfoLog.offset
		raw = updateInfoLog.oldRaw
	}

	page, err := pc.GetPage(int(pageNumber))
	if err != nil {
		panic(err)
	}
	dmPage.PageXRecoverInsert(page, raw, offset)
	page.Release()
}

// redoTransactions 遍历事务，根据事务的状态决定是否要进行redo操作(包括了插入的redo和更新的redo)
func redoTransactions(tm *tm.TransactionManagerImpl, lg *logger.DBLogger, pc *dmPage.PageCache) {
	lg.Rewind()
	for {
		log := lg.Next()
		if log == nil {
			break
		}
		if IsInsertLog(log) {
			insertInfoLog := parseInsertLog(log)
			xid := insertInfoLog.xid
			if !tm.IsActive(xid) {
				doInsertLog(pc, log, TypeRedo)
			}
		} else {
			updateInfoLog := parseUpdateLog(log)
			xid := updateInfoLog.xid
			if !tm.IsActive(xid) {
				doUpdateLog(pc, log, TypeRedo)
			}
		}
	}
}

func undoTransactions(tm *tm.TransactionManagerImpl, lg *logger.DBLogger, pc *dmPage.PageCache) {
	lg.Rewind()
	logCache := make(map[int64][][]byte)
	for {
		log := lg.Next()
		if log == nil {
			break
		}
		if IsInsertLog(log) {
			insertInfoLog := parseInsertLog(log)
			xid := insertInfoLog.xid
			if tm.IsActive(xid) {
				logCache[xid] = append(logCache[xid], log)
			}
		} else {
			updateInfoLog := parseUpdateLog(log)
			xid := updateInfoLog.xid
			if tm.IsActive(xid) {
				logCache[xid] = append(logCache[xid], log)
			}
		}
	}

	// 对所有上面记录的事务进行undo操作
	for xid, logs := range logCache {
		for i := len(logs) - 1; i >= 0; i-- {
			log := logs[i]
			if IsInsertLog(log) {
				doInsertLog(pc, log, TypeUndo)
			} else {
				doUpdateLog(pc, log, TypeUndo)
			}
		}
		tm.Abort(xid)
	}

}

// Recover 根据日志恢复数据库状态
func Recover(tm *tm.TransactionManagerImpl, lg *logger.DBLogger, pc *dmPage.PageCache) {
	commons.Logger.Infof("Recover start.......")

	lg.Rewind()
	maxPageNumber := 0
	for {
		log := lg.Next()
		if log == nil {
			break
		}
		var pageNumber int
		if IsInsertLog(log) {
			insertInfoLog := parseInsertLog(log)
			pageNumber = insertInfoLog.pageNumber
		} else {
			updateInfoLog := parseUpdateLog(log)
			pageNumber = updateInfoLog.pageNumber
		}
		if pageNumber > maxPageNumber {
			maxPageNumber = pageNumber
		}
	}

	if maxPageNumber == 0 {
		// 这里是因为没有日志，所以没有任何数据需要恢复，但是存在一个用于校验的PageOne
		maxPageNumber = 1
	}

	// 截断后面的无效日志
	pc.TruncateByPgNo(maxPageNumber)
	commons.Logger.Infof("Truncate to page %d", maxPageNumber)

	redoTransactions(tm, lg, pc)
	commons.Logger.Infof("Redo done.......")

	undoTransactions(tm, lg, pc)
	commons.Logger.Infof("Undo done.......")

	commons.Logger.Infof("Recover done.......")

}
