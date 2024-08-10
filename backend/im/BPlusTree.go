package im

import (
	"SimpleDB/backend/dm"
	"SimpleDB/backend/tm"
	"SimpleDB/commons"
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

type BPlusTree struct {
	DM           *dm.DataManager
	BootUid      int64
	BootDataItem *dm.DataItem
	BootLock     sync.Locker
}

// CreateBPlusTree 创建一个B+树，将根节点插入到数据管理器中
func CreateBPlusTree(dm *dm.DataManager) (int64, error) {
	rawRoot := NewNilRootRaw()
	// 将一个根节点插入到数据管理器中
	rootUid, err := dm.Insert(tm.SuperXid, rawRoot)
	if err != nil {
		return 0, err
	}
	rootUidByte := make([]byte, 8)
	// 将根节点的uid转换为字节数组
	binary.BigEndian.PutUint64(rootUidByte, uint64(rootUid))
	// 将根节点的uid插入到数据管理器中
	bootUid, err := dm.Insert(tm.SuperXid, rootUidByte)
	if err != nil {
		return 0, err
	}
	return bootUid, nil
}

// LoadBPlusTree 从数据管理器中加载一个B+树
func LoadBPlusTree(bootUid int64, dm *dm.DataManager) (*BPlusTree, error) {
	bootDataItem := dm.Read(bootUid)
	return &BPlusTree{
		DM:           dm,
		BootUid:      bootUid,
		BootDataItem: bootDataItem,
		BootLock:     &commons.ReentrantLock{},
	}, nil
}

// rootUid 获取根节点的uid
func (bTree *BPlusTree) rootUid() int64 {
	bTree.BootLock.Lock()
	defer bTree.BootLock.Unlock()
	return int64(binary.BigEndian.Uint64(bTree.BootDataItem.Data()))
}

// updateRootUid 更新根节点的uid
func (bTree *BPlusTree) updateRootUid(left int64, right int64, rightKey int64) error {
	bTree.BootLock.Lock()
	defer bTree.BootLock.Unlock()

	rootRaw := NewRootRaw(left, right, rightKey)
	newRootUid, err := bTree.DM.Insert(tm.SuperXid, rootRaw)
	if err != nil {
		return err
	}
	bTree.BootDataItem.Before()

	dataItemRaw := bTree.BootDataItem.Data()
	binary.BigEndian.PutUint64(dataItemRaw, uint64(newRootUid))
	bTree.BootDataItem.After(tm.SuperXid)
	return nil
}

func (bTree *BPlusTree) searchLeaf(nodeUid int64, key int64) (int64, error) {
	node, err := LoadNode(bTree, nodeUid)
	if err != nil {
		return 0, err
	}
	isLeaf := node.IsLeaf()
	node.Release()

	if isLeaf {
		return nodeUid, nil
	} else {
		next, err := bTree.searchNext(nodeUid, key)
		if err != nil {
			return 0, err
		}
		return bTree.searchLeaf(next, key)
	}

}

// searchNext 从一个节点开始搜索下一个节点
func (bTree *BPlusTree) searchNext(nodeUid int64, key int64) (int64, error) {
	for {
		node, err := LoadNode(bTree, nodeUid)
		if err != nil {
			return 0, err
		}
		res := node.SearchNext(key)
		node.Release()
		if res.Uid != 0 {
			return res.Uid, nil
		}
		nodeUid = res.SiblingUid
	}
}

// Search 从B+树中搜索一个key
func (bTree *BPlusTree) Search(key int64) ([]int64, error) {
	return bTree.SearchRange(key, key)
}

// SearchRange 从B+树中搜索一个范围
func (bTree *BPlusTree) SearchRange(leftKey int64, rightKey int64) ([]int64, error) {
	rootUid := bTree.rootUid()
	// 要从叶子节点开始搜索，利用了B+树的特点，叶子节点的key是有序的并且可以连起来
	leafUid, err := bTree.searchLeaf(rootUid, leftKey)
	if err != nil {
		return nil, err
	}
	// 存储结果的数组
	uids := make([]int64, 0)
	for {
		leaf, err := LoadNode(bTree, leafUid)
		if err != nil {
			return nil, err
		}
		// 从叶子节点开始顺序查找。叶子节点的key是有序的
		leafSearchRangeResult := leaf.LeafSearchRange(leftKey, rightKey)
		leaf.Release()
		uids = append(uids, leafSearchRangeResult.Uids...)
		if leafSearchRangeResult.SiblingUid == 0 {
			break
		} else {
			leafUid = leafSearchRangeResult.SiblingUid
		}
	}
	return uids, nil

}

// Insert 向B+树中插入一个键值对
func (bTree *BPlusTree) Insert(key int64, uid int64) error {
	rootUid := bTree.rootUid()
	insertResult, err := bTree.insert(rootUid, uid, key)
	if err != nil {
		return err
	}
	if insertResult.newNode != 0 {
		err = bTree.updateRootUid(rootUid, insertResult.newNode, insertResult.newKey)
		if err != nil {
			return err
		}
	}
	return nil

}

type InsertResult struct {
	newNode int64
	newKey  int64
}

func (bTree *BPlusTree) insert(nodeUid int64, uid int64, key int64) (*InsertResult, error) {
	node, err := LoadNode(bTree, nodeUid)
	if err != nil {
		return nil, err
	}
	isLeaf := node.IsLeaf()
	node.Release()
	var insertResult *InsertResult
	if isLeaf {
		insertResult, err = bTree.insertAndSplit(nodeUid, uid, key)
		if err != nil {
			return nil, err
		}
	} else {
		next, err := bTree.searchNext(nodeUid, key)
		if err != nil {
			return nil, err
		}
		ir, err := bTree.insert(next, uid, key)
		if err != nil {
			return nil, err
		}
		if ir.newNode != 0 {
			insertResult, err = bTree.insertAndSplit(nodeUid, ir.newNode, ir.newKey)
			if err != nil {
				return nil, err
			}
		} else {
			insertResult = &InsertResult{}
		}
	}
	return insertResult, nil
}

func (bTree *BPlusTree) insertAndSplit(nodeUid int64, uid int64, key int64) (*InsertResult, error) {
	for {
		node, err := LoadNode(bTree, nodeUid)
		if err != nil {
			return nil, err
		}
		insertResult, err := node.InsertAndSplit(uid, key)
		node.Release()
		if err != nil {
			return nil, err
		}
		if insertResult.SiblingUid != 0 {
			nodeUid = insertResult.SiblingUid
		} else {
			return &InsertResult{
				newNode: insertResult.NewSon,
				newKey:  insertResult.NewKey,
			}, nil
		}
	}
}

func (bTree *BPlusTree) Close() {
	bTree.BootDataItem.Release()
}

// String 方法用于将 BPlusTree 的详细信息转换为字符串
func (bTree *BPlusTree) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("BPlusTree Root UID: %d\n", bTree.rootUid()))

	// 遍历所有节点并将其信息添加到缓冲区中
	nodeUidQueue := []int64{bTree.rootUid()}
	visited := make(map[int64]bool)
	for len(nodeUidQueue) > 0 {
		currentUid := nodeUidQueue[0]
		nodeUidQueue = nodeUidQueue[1:]

		if visited[currentUid] {
			continue
		}
		visited[currentUid] = true

		node, err := LoadNode(bTree, currentUid)
		if err != nil {
			buffer.WriteString(fmt.Sprintf("Failed to load node UID: %d, error: %s\n", currentUid, err))
			continue
		}

		buffer.WriteString(node.String())
		buffer.WriteString("\n")

		numberKeys := GetRawNumberKeys(node.Raw)
		for i := 0; i <= numberKeys; i++ {
			childUid := GetRawKthSon(node.Raw, i)
			if childUid != 0 && !GetRawIsLeaf(node.Raw) {
				nodeUidQueue = append(nodeUidQueue, childUid)
			}
		}

		node.Release()
	}

	return buffer.String()
}
