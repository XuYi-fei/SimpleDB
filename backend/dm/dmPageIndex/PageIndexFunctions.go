package dmPageIndex

// Add 添加页的索引信息
// 根据给定的页面编号和空闲空间大小添加一个 PageInfo 对象
func (pageIndex *PageIndex) Add(pageNumber int32, freeSpace int32) {
	pageIndex.mu.Lock()
	defer pageIndex.mu.Unlock()

	// 使用页的剩余空间计算应该添加哪个索引
	// 计算空闲空间大小对应的区间编号
	interval := freeSpace / IntervalSize
	// 在对应的区间列表中添加一个新的 PageInfo 对象
	pageIndex.lists[interval] = append(pageIndex.lists[interval], &PageInfo{
		PageNumber: pageNumber,
		FreeSpace:  freeSpace,
	})
}

// Select 选择一个页
// 根据给定的空间大小选择一个 PageInfo 结构体
// 返回一个 PageInfo 对象，其空闲空间大于或等于给定的空间大小。如果没有找到合适的 PageInfo，返回 nil
func (pageIndex *PageIndex) Select(spaceSize int32) *PageInfo {
	pageIndex.mu.Lock()
	defer pageIndex.mu.Unlock()

	// 计算需要的空间大小对应的区间编号
	// 此处+1主要为了向上取整
	number := spaceSize / IntervalSize
	// 如果计算出的区间编号小于总的区间数，编号加一
	if number < IntervalsNumber {
		number++
	}
	// 从计算出的区间编号开始，向上寻找合适的 PageInfo
	for ; number <= IntervalsNumber; number++ {
		// 如果当前区间没有 PageInfo，继续查找下一个区间
		if len(pageIndex.lists[number]) > 0 {
			pageInfo := pageIndex.lists[number][0]
			pageIndex.lists[number] = pageIndex.lists[number][1:]
			return pageInfo
		}
	}
	// 如果没有找到合适的 PageInfo，返回 nil
	return nil
}
