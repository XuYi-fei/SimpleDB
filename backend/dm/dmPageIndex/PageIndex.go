package dmPageIndex

import (
	"dbofmine/backend/dm/constants"
	"dbofmine/commons"
)

var (
	// IntervalsNumber 将一页划分为40个区间
	IntervalsNumber int32 = 40
	// IntervalSize 区间大小
	IntervalSize int32 = int32(constants.PageSize) / IntervalsNumber
)

type PageIndex struct {
	mu commons.ReentrantLock
	// lists 二维切片，第一维表示区间，第二维表示区间内的页
	lists [][]*PageInfo
}

func NewPageIndex() *PageIndex {
	lists := make([][]*PageInfo, IntervalsNumber+1)
	for i := 0; i < int(IntervalsNumber+1); i++ {
		lists[i] = make([]*PageInfo, 0)
	}
	return &PageIndex{
		lists: lists,
	}
}
