package im

import (
	"SimpleDB/backend/dm"
	"SimpleDB/backend/tm"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid] ---> Node Header
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */

var (
	// IsLeftOffset 是否是叶子节点
	IsLeftOffset = 0
	// NumberKeysOffset 关键字个数的偏移位置
	NumberKeysOffset = IsLeftOffset + 1
	// SiblingOffset 兄弟节点的偏移位置
	SiblingOffset = NumberKeysOffset + 2
	// NodeHeaderSize 节点头部大小
	NodeHeaderSize = SiblingOffset + 8

	// BalanceNumber 节点的平衡因子的常量，一个节点最多可以包含32个key
	BalanceNumber = 32

	// NodeSize 节点大小，一个节点最多可以包含32个key和32个Son(从0-32其实是33个，所以后面要加2)，每个key和Son各占用8个字节
	NodeSize = NodeHeaderSize + (2*8)*(BalanceNumber*2+2)
)

// Node B+树的节点表示
type Node struct {
	Tree     *BPlusTree
	DataItem *dm.DataItem
	Raw      []byte
	Uid      int64
}

// SetRawIsLeaf 设置是否为叶子节点，1表示是叶子节点，0表示非叶子节点
func SetRawIsLeaf(raw []byte, isLeaf bool) {
	if isLeaf {
		raw[IsLeftOffset] = 1
	} else {
		raw[IsLeftOffset] = 0
	}
}

// GetRawIsLeaf 判断是否是叶子节点
func GetRawIsLeaf(raw []byte) bool {
	return raw[IsLeftOffset] == 1
}

// SetRawNumberKeys 设置节点个数
func SetRawNumberKeys(raw []byte, numberKeys int) {
	binary.BigEndian.PutUint16(raw[NumberKeysOffset:NumberKeysOffset+2], uint16(numberKeys))
}

// GetRawNumberKeys 获取节点个数
func GetRawNumberKeys(raw []byte) int {
	return int(binary.BigEndian.Uint16(raw[NumberKeysOffset : NumberKeysOffset+2]))
}

// SetRawSibling 设置兄弟节点的UID，占用8字节
func SetRawSibling(raw []byte, sibling int64) {
	binary.BigEndian.PutUint64(raw[SiblingOffset:SiblingOffset+8], uint64(sibling))
}

// GetRawSibling 获取兄弟节点的UID
func GetRawSibling(raw []byte) int64 {
	return int64(binary.BigEndian.Uint64(raw[SiblingOffset : SiblingOffset+8]))
}

// SetRawKthSon 设置第k个子节点的UID 注意k是从0开始的
func SetRawKthSon(raw []byte, uid int64, kth int) {
	offset := NodeHeaderSize + kth*(8*2)
	binary.BigEndian.PutUint64(raw[offset:offset+8], uint64(uid))
}

// GetRawKthSon 获取第k个子节点的UID 注意k是从0开始的
func GetRawKthSon(raw []byte, kth int) int64 {
	offset := NodeHeaderSize + kth*(8*2)
	return int64(binary.BigEndian.Uint64(raw[offset : offset+8]))
}

// SetRawKthKey 设置第k个key的值 注意k是从0开始的
func SetRawKthKey(raw []byte, key int64, kth int) {
	offset := NodeHeaderSize + kth*(8*2) + 8
	binary.BigEndian.PutUint64(raw[offset:offset+8], uint64(key))
}

// GetRawKthKey 获取第k个key的值 注意k是从0开始的
func GetRawKthKey(raw []byte, kth int) int64 {
	offset := NodeHeaderSize + kth*(8*2) + 8
	return int64(binary.BigEndian.Uint64(raw[offset : offset+8]))
}

// CopyRawFromKth 从一个节点的原始字节数组中复制一部分数据到另一个节点的原始字节数组中
func CopyRawFromKth(from []byte, to []byte, kth int) {
	offset := NodeHeaderSize + kth*(8*2)
	// 将源节点的原始字节数组中的数据复制到目标节点的原始字节数组中
	// 复制的数据包括从起始位置到源节点的原始字节数组的末尾的所有数据
	copy(to[NodeHeaderSize:], from[offset:])
}

// ShiftRawKth 将一个节点的原始字节数组中的节点整体向后移动
func ShiftRawKth(raw []byte, kth int) {
	begin := NodeHeaderSize + (kth+1)*(8*2)
	end := NodeSize - 1
	for i := end; i >= begin; i-- {
		raw[i] = raw[i-8*2]
	}
}

// NewRootRaw 创建一个新的根节点的原始字节数组
// 这个新的根节点包含两个子节点，它们的键分别是key和MaxInt64，UID分别是left和right
func NewRootRaw(left int64, right int64, key int64) []byte {
	// 创建一个新的字节数组，大小为节点的大小
	raw := make([]byte, NodeSize)
	// 设置节点为非叶子节点
	SetRawIsLeaf(raw, false)
	// 设置节点的键的数量为2
	SetRawNumberKeys(raw, 2)
	// 设置节点的兄弟节点的UID为0
	SetRawSibling(raw, 0)
	// 设置第0个子节点的UID为left
	SetRawKthSon(raw, left, 0)
	// 设置第0个键的值为key
	SetRawKthKey(raw, key, 0)
	// 设置第1个子节点的UID为right
	SetRawKthSon(raw, right, 1)
	// 设置第1个键的值为
	SetRawKthKey(raw, math.MaxInt64, 1)

	// 返回新创建的根节点的原始字节数组
	return raw
}

// NewNilRootRaw 创建一个新的空根节点的原始字节数组，这个新的根节点没有子节点和键
func NewNilRootRaw() []byte {
	// 创建一个新的字节数组，大小为节点的大小
	raw := make([]byte, NodeSize)
	// 设置节点为叶子节点
	SetRawIsLeaf(raw, true)
	// 设置节点的键的数量为0
	SetRawNumberKeys(raw, 0)
	// 设置节点的兄弟节点的UID为0
	SetRawSibling(raw, 0)

	return raw
}

// LoadNode 根据uid加载一个节点
func LoadNode(bTree *BPlusTree, uid int64) (*Node, error) {
	dataItem := bTree.DM.Read(uid)
	// DataItem 理论上不为空
	if dataItem == nil {
		return nil, errors.New("LoadNode: DataItem is nil")
	}
	node := &Node{
		Tree:     bTree,
		DataItem: dataItem,
		Raw:      dataItem.Data(),
		Uid:      uid,
	}
	return node, nil
}

func (node *Node) Release() {
	node.DataItem.Release()
}

func (node *Node) IsLeaf() bool {
	node.DataItem.RLock()
	defer node.DataItem.RUnLock()

	return GetRawIsLeaf(node.Raw)
}

// ============ 用于在B+树的节点中查找插入下一个节点的位置 =================

type SearchNextResult struct {
	Uid        int64
	SiblingUid int64
}

// SearchNext 在B+树的节点中搜索下一个节点的方法
// 搜索的逻辑是给定当前的key，要找到当前节点中第一个大于key的已有的key
func (node *Node) SearchNext(key int64) *SearchNextResult {
	// 获取节点的读锁
	node.DataItem.RLock()
	defer node.DataItem.RUnLock()

	// 创建一个SearchNextRes对象，用于存储搜索结果
	result := &SearchNextResult{}
	// 获取节点个数
	numberKeys := GetRawNumberKeys(node.Raw)
	for i := 0; i < numberKeys; i++ {
		// 获取第i个key的值
		ik := GetRawKthKey(node.Raw, i)
		// 如果当前的key大于给定的key，则返回
		if ik > key {
			// 设置下一个节点的UID
			result.Uid = GetRawKthSon(node.Raw, i)
			// 设置兄弟节点的UID为0
			result.SiblingUid = 0
			// 返回搜索结果
			return result
		}
	}
	// 如果没有找到下一个节点，设置uid为0
	result.Uid = 0
	// 设置兄弟节点的UID为当前节点的兄弟节点的UID
	result.SiblingUid = GetRawSibling(node.Raw)
	// 返回搜索结果
	return result
}

// ============ 用于在B+树中根据key搜索节点 =================

type LeafSearchRangeResult struct {
	Uids       []int64
	SiblingUid int64
}

// LeafSearchRange 在B+树的叶子节点中搜索一个范围的key
func (node *Node) LeafSearchRange(leftKey int64, rightKey int64) *LeafSearchRangeResult {
	node.DataItem.RLock()
	defer node.DataItem.RUnLock()

	// 获取节点中的键的数量
	numberKeys := GetRawNumberKeys(node.Raw)
	kth := 0

	// 找到第一个大于或等于左键的键
	for kth < numberKeys {
		ik := GetRawKthKey(node.Raw, kth)
		if ik >= leftKey {
			break
		}
		kth++
	}

	// 创建一个列表，用于存储所有在键值范围内的子节点的UID
	uids := make([]int64, 0)
	// 遍历所有的键，将所有小于或等于右键的键对应的子节点的UID添加到列表中
	for kth < numberKeys {
		ik := GetRawKthKey(node.Raw, kth)
		if ik > rightKey {
			break
		}
		uids = append(uids, GetRawKthSon(node.Raw, kth))
		kth++
	}
	// 如果所有的键都被遍历过，获取兄弟节点的UID
	var siblingUid int64
	if kth == numberKeys {
		siblingUid = GetRawSibling(node.Raw)
	}
	// 创建一个LeafSearchRangeRes对象，用于存储搜索结果
	result := &LeafSearchRangeResult{
		Uids:       uids,
		SiblingUid: siblingUid,
	}

	return result
}

// ============ 用于在B+树的节点中插入一个节点，并在需要时分裂节点 =================

type InsertAndSplitResult struct {
	SiblingUid int64
	NewSon     int64
	NewKey     int64
}

// InsertAndSplit 在B+树的节点中插入一个键值对，并在需要时分裂节点
func (node *Node) InsertAndSplit(uid int64, key int64) (*InsertAndSplitResult, error) {
	// 创建一个标志位，用于标记插入操作是否成功
	success := false
	// 创建一个异常对象，用于存储在插入或分裂节点时发生的异常
	//var err error = nil
	// 创建一个InsertAndSplitRes对象，用于存储插入和分裂节点的结果
	result := &InsertAndSplitResult{}

	// 在数据项上设置一个保存点
	node.DataItem.Before()

	// 尝试在节点中插入键值对，并获取插入结果
	success = node.insert(uid, key)
	// 如果插入失败，设置兄弟节点的UID，并返回结果
	if !success {
		result.SiblingUid = GetRawSibling(node.Raw)
		// 如果发生错误或插入失败，回滚数据项的修改
		node.DataItem.UnBefore()
		return result, nil
	}
	// 如果需要分裂节点
	if node.needSplit() {
		// 分裂节点，并获取分裂结果
		splitResult, err := node.split()
		if err != nil {
			// 如果发生错误，回滚数据项的修改
			node.DataItem.UnBefore()
			return nil, err
		}
		// 设置新节点的UID和新键，并返回结果
		result.NewSon = splitResult.newSon
		result.NewKey = splitResult.newKey
		node.DataItem.After(tm.SuperXid)
		return result, nil
	} else {
		// 如果不需要分裂节点，提交数据项的修改
		node.DataItem.After(tm.SuperXid)
		return result, nil
	}

}

// insert 在B+树的节点中插入一个键值对的方法
func (node *Node) insert(uid int64, key int64) bool {
	// 获取节点中的键的数量
	numberKeys := GetRawNumberKeys(node.Raw)
	// 初始化插入位置的索引
	kth := 0
	// 找到第一个大于或等于要插入的键的键的位置
	for kth < numberKeys {
		ik := GetRawKthKey(node.Raw, kth)
		if ik >= key {
			break
		}
		kth++
	}
	// 如果所有的键都被遍历过，并且存在兄弟节点，插入失败
	if kth == numberKeys && GetRawSibling(node.Raw) != 0 {
		return false
	}

	// 如果节点是叶子节点
	if GetRawIsLeaf(node.Raw) {
		// 在插入位置后的所有键和子节点向后移动一位
		ShiftRawKth(node.Raw, kth)
		// 在插入位置插入新的键和子节点的UID
		SetRawKthKey(node.Raw, key, kth)
		SetRawKthSon(node.Raw, uid, kth)
		// 更新节点中的键的数量
		SetRawNumberKeys(node.Raw, numberKeys+1)
	} else {
		// 如果节点是非叶子节点
		// 获取插入位置的键
		kk := GetRawKthKey(node.Raw, kth)
		// 在插入位置插入新的键
		SetRawKthKey(node.Raw, key, kth)
		// 在插入位置后的所有键和子节点向后移动一位
		ShiftRawKth(node.Raw, kth)
		// 在插入位置的下一个位置插入原来的键和新的子节点的UID
		SetRawKthKey(node.Raw, kk, kth+1)
		SetRawKthSon(node.Raw, uid, kth+1)
		// 更新节点中的键的数量
		SetRawNumberKeys(node.Raw, numberKeys+1)
	}

	return true
}

type SplitResult struct {
	newSon int64
	newKey int64
}

// needSplit 判断节点是否需要分裂
func (node *Node) needSplit() bool {
	return GetRawNumberKeys(node.Raw) == BalanceNumber*2
}

// split 分裂B+树的节点
// 当一个节点的键的数量达到 BALANCE_NUMBER * 2 时，就意味着这个节点已经满了，需要进行分裂操作
// 分裂操作的目的是将一个满的节点分裂成两个节点，每个节点包含一半的键
func (node *Node) split() (*SplitResult, error) {
	// 创建一个新的字节数组，用于存储新节点的原始数据
	nodeRaw := make([]byte, NodeSize)
	// 设置新节点的叶子节点标志，与原节点相同
	SetRawIsLeaf(nodeRaw, GetRawIsLeaf(node.Raw))
	// 设置新节点的键的数量为BALANCE_NUMBER
	SetRawNumberKeys(nodeRaw, BalanceNumber)
	// 设置新节点的兄弟节点的UID，与原节点的兄弟节点的UID相同
	SetRawSibling(nodeRaw, GetRawSibling(node.Raw))
	// 从原节点的原始字节数组中复制一部分数据到新节点的原始字节数组中
	CopyRawFromKth(node.Raw, nodeRaw, BalanceNumber)
	// 在数据管理器中插入新节点的原始数据，并获取新节点的UID
	son, err := node.Tree.DM.Insert(tm.SuperXid, nodeRaw)
	if err != nil {
		return nil, err
	}
	// 更新原节点的键的数量为BALANCE_NUMBER
	SetRawNumberKeys(node.Raw, BalanceNumber)
	// 更新原节点的兄弟节点的UID为新节点的UID
	SetRawSibling(node.Raw, son)

	// 创建一个SplitRes对象，用于存储分裂结果
	result := &SplitResult{
		// 设置新节点的UID
		newSon: son,
		// 设置新键为新节点的第一个键的值
		newKey: GetRawKthKey(nodeRaw, 0),
	}
	return result, nil
}

// String 方法用于将 Node 的详细信息转换为字符串
func (node *Node) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Node UID: %d\n", node.Uid))
	buffer.WriteString(fmt.Sprintf("Is Leaf: %t\n", GetRawIsLeaf(node.Raw)))
	buffer.WriteString(fmt.Sprintf("Number of Keys: %d\n", GetRawNumberKeys(node.Raw)))
	buffer.WriteString(fmt.Sprintf("Sibling UID: %d\n", GetRawSibling(node.Raw)))

	numberKeys := GetRawNumberKeys(node.Raw)
	for i := 0; i < numberKeys; i++ {
		buffer.WriteString(fmt.Sprintf("Key[%d]:%d | ", i, GetRawKthKey(node.Raw, i)))
		buffer.WriteString(fmt.Sprintf("Son[%d]:%d | ", i, GetRawKthSon(node.Raw, i)))
	}
	if !GetRawIsLeaf(node.Raw) {
		buffer.WriteString(fmt.Sprintf("Son[%d]:%d | \n", numberKeys, GetRawKthSon(node.Raw, numberKeys)))
	}

	return buffer.String()
}
