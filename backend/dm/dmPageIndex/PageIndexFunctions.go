package dmPageIndex

// Add 添加页的索引信息
func (pageIndex *PageIndex) Add(pageNumber int32, freeSpace int32) {
	pageIndex.mu.Lock()
	defer pageIndex.mu.Unlock()

	// 使用页的剩余空间计算应该添加哪个索引
	interval := freeSpace / IntervalSize
	pageIndex.lists[interval] = append(pageIndex.lists[interval], &PageInfo{
		PageNumber: pageNumber,
		FreeSpace:  freeSpace,
	})

}

// Select 选择一个页
func (pageIndex *PageIndex) Select(spaceSize int32) *PageInfo {
	pageIndex.mu.Lock()
	defer pageIndex.mu.Unlock()

	number := spaceSize / IntervalSize
	if number < IntervalsNumber {
		number++
	}
	for ; number <= IntervalsNumber; number++ {
		if len(pageIndex.lists[number]) > 0 {
			pageInfo := pageIndex.lists[number][0]
			pageIndex.lists[number] = pageIndex.lists[number][1:]
			return pageInfo
		}
	}
	return nil
}
