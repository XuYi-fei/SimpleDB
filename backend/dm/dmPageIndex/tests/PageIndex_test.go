package tests

import (
	"dbofmine/backend/dm/constants"
	"dbofmine/backend/dm/dmPageIndex"
	"testing"
)

func TestPageIndex(t *testing.T) {
	t.Log("PageIndex test")
	pageIndex := dmPageIndex.NewPageIndex()
	threshold := constants.PageSize / 20
	for i := 0; i < 20; i++ {
		pageIndex.Add(int32(i), int32(i*threshold))
		pageIndex.Add(int32(i), int32(i*threshold))
		pageIndex.Add(int32(i), int32(i*threshold))
	}

	for i := 0; i < 3; i++ {
		for k := 0; k < 19; k++ {
			pageInfo := pageIndex.Select(int32(k * threshold))
			if pageInfo == nil {
				panic("Select error")
			}
			if int(pageInfo.PageNumber) != k+1 {
				t.Error("Select error")
			}
		}
	}
}
