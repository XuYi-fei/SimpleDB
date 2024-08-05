package tests

import (
	"SimpleDB/backend/dm"
	"SimpleDB/backend/dm/constants"
	"SimpleDB/backend/im"
	"SimpleDB/backend/tm"
	"fmt"
	"os"
	"testing"
)

func TestTreeSingle(t *testing.T) {
	t.Log("TestB+TreeSingle")
	tm, _ := tm.CreateTransactionManagerImpl("/Users/xuyifei/repos/SimpleDB/data/test/backend/im/TestTreeSingle")
	dm := dm.CreateDataManager("/Users/xuyifei/repos/SimpleDB/data/test/backend/im/TestTreeSingle", int64(constants.PageSize*10), tm)

	root, _ := im.CreateBPlusTree(dm)
	tree, _ := im.LoadBPlusTree(root, dm)

	limit := 10000
	for i := limit - 1; i >= 0; i-- {
		tree.Insert(int64(i), int64(i))
	}

	fmt.Print(tree.String())

	for i := 0; i < limit; i++ {
		uids, _ := tree.Search(int64(i))
		if len(uids) != 1 {
			t.Errorf("Search %d failed", i)
		}
		if uids[0] != int64(i) {
			t.Errorf("uid[0] is %d, not equal to %d", uids[0], i)
		}
	}

	os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/im/TestTreeSingle.db")
	os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/im/TestTreeSingle.xid")
	os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/im/TestTreeSingle.log")

}
