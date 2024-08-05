package tbm

import (
	"dbofmine/backend/im"
	"dbofmine/backend/parser/statement"
	"dbofmine/backend/tm"
	"dbofmine/commons"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
)

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 */

type Field struct {
	// 唯一标识符，用于标识每个Field对象
	Uid int64
	// Field对象所属的表
	table *Table
	// 字段名，用于标识表中的每个字段
	FieldName string
	// 字段类型，用于标识字段的数据类型
	FieldType string
	// 索引，用于标识字段是否有索引，如果索引为0，表示没有索引
	index int64
	// B+树，用于存储索引，如果字段有索引，这个B+树会被加载
	bt *im.BPlusTree
}

/**
 * 创建一个新的Field对象
 * tb        表对象，Field对象所属的表
 * Xid       事务ID
 * fieldName 字段名
 * fieldType 字段类型
 * indexed   是否创建索引
 */

// CreateField 创建一个新的Field对象
func CreateField(tb *Table, xid int64, fieldName string, fieldType string, indexed bool) (*Field, error) {
	// 检查字段类型是否有效
	err := fieldTypeCheck(fieldType)
	if err != nil {
		return nil, err
	}
	// 创建一个新的Field对象
	f := &Field{
		table:     tb,
		FieldName: fieldName,
		FieldType: fieldType,
		index:     0,
	}
	// 如果需要创建索引
	if indexed {
		// 创建一个新的B+树索引
		indexUid, err := im.CreateBPlusTree(tb.TBM.DM)
		if err != nil {
			return nil, err
		}
		// 加载这个B+树索引
		bt, err := im.LoadBPlusTree(indexUid, tb.TBM.DM)
		if err != nil {
			return nil, err
		}
		// 设置Field对象的索引
		f.index = indexUid
		// 设置Field对象的B+树
		f.bt = bt
	}
	err = f.persistSelf(xid)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// LoadField 从持久化存储中加载一个Field对象
func LoadField(tb *Table, uid int64) *Field {
	// 用于存储从持久化存储中读取的原始字节数据
	// 从持久化存储中读取uid对应的原始字节数据
	raw, err := tb.TBM.VM.Read(tm.SUPER_XID, uid)
	if err != nil {
		// 如果读取过程中出现异常，调用panic方法处理异常
		panic(err)
	}
	if raw == nil {
		// 如果读取的数据为空，调用panic方法处理异常
		panic("原始字节数据不为nil，如果为nil，那么会抛出AssertionError")
	}
	// 创建一个新的Field对象，并调用parseSelf方法解析原始字节数据
	field := &Field{
		table: tb,
		Uid:   uid,
	}
	field.parseSelf(raw)
	return field
}

// parseSelf 解析原始字节数组并设置字段名、字段类型和索引
func (field *Field) parseSelf(raw []byte) *Field {
	// 初始化位置为0
	pos := 0
	// 解析原始字节数组，获取字段名和下一个位置
	parseStringResult := commons.ParseString(raw)
	field.FieldName = parseStringResult.Str
	pos += int(parseStringResult.Next)

	// 从新的位置开始解析原始字节数组，获取字段类型和下一个位置
	parseStringResult = commons.ParseString(raw[pos:])
	// 设置字段类型
	field.FieldType = parseStringResult.Str
	// 更新位置
	pos += int(parseStringResult.Next)

	// 从新的位置开始解析原始字节数组，获取索引
	index := binary.BigEndian.Uint64(raw[pos : pos+8])
	// 如果索引不为0，说明存在B+树索引
	if index != 0 {
		// 加载B+树索引
		bt, err := im.LoadBPlusTree(int64(index), field.table.TBM.DM)
		if err != nil {
			panic(err)
		}
		field.bt = bt
		field.index = int64(index)
	}
	return field
}

// persistSelf 将当前Field对象持久化到存储中
func (field *Field) persistSelf(xid int64) error {
	// 将字段名转换为字节数组
	fieldNameBytes := commons.String2Bytes(field.FieldName)
	// 将字段类型转换为字节数组
	fieldTypeBytes := commons.String2Bytes(field.FieldType)
	// 将索引转换为字节数组
	indexBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(indexBytes, uint64(field.index))
	// 将字段名、字段类型和索引的字节数组合并，然后插入到持久化存储中
	data := commons.BytesConcat(fieldNameBytes, fieldTypeBytes, indexBytes)
	// 插入成功后，会返回一个唯一的uid，将这个uid设置为当前Field对象的uid
	uid, err := field.table.TBM.VM.Insert(xid, data)
	if err != nil {
		return err
	}
	field.Uid = uid
	return nil
}

// fieldTypeCheck 检查字段类型是否合法
func fieldTypeCheck(fieldType string) error {
	if fieldType == "int32" || fieldType == "string" || fieldType == "int64" {
		return nil
	}
	return errors.New(commons.ErrorMessage.InvalidFieldTypeError)
}

func (field *Field) IsIndexed() bool {
	return field.index != 0
}

// Insert 将key和uid插入到B+树索引中
func (field *Field) Insert(key interface{}, uid int64) error {
	uKey := field.Value2Uid(key)
	err := field.bt.Insert(uKey, uid)
	if err != nil {
		return err
	}
	return nil
}

// Search 根据key的范围查找uid
func (field *Field) Search(left int64, right int64) ([]int64, error) {
	return field.bt.SearchRange(left, right)
}

func (field *Field) string2Value(str string) interface{} {
	switch field.FieldType {
	case "string":
		return str
	case "int32":
		num, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			panic(err)
		}
		return int32(num)
	case "int64":
		num, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			panic(err)
		}
		return int64(num)
	}
	return nil
}

func (field *Field) String2Value(str string) interface{} {
	switch field.FieldType {
	case "string":
		return str
	case "int32":
		num, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			panic(err)
		}
		return int32(num)
	case "int64":
		num, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			panic(err)

		}
		return num
	}
	return nil
}

// Value2Uid 根据key生成uid
func (field *Field) Value2Uid(key interface{}) int64 {
	var uid int64 = 0
	switch field.FieldType {
	case "string":
		uid = commons.Str2Uid(key.(string))
		break
	case "int32":
		uint := int(key.(int32))
		uid = int64(uint)
		break
	case "int64":
		uid = key.(int64)
		break
	}
	return uid
}

// Value2Raw 将value转换为原始字节数组
func (field *Field) Value2Raw(v interface{}) []byte {
	var raw []byte
	switch field.FieldType {
	case "string":
		raw = commons.String2Bytes(v.(string))
		break
	case "int32":
		raw = make([]byte, 4)
		binary.BigEndian.PutUint32(raw, uint32(v.(int32)))
		break
	case "int64":
		raw = make([]byte, 8)
		binary.BigEndian.PutUint64(raw, uint64(v.(int64)))
		break
	}
	return raw
}

// CalFieldResult 用于对某个字段查询后的计算结果
type CalFieldResult struct {
	left  int64
	right int64
}

// CalExp 根据条件查询表达式得到查询的结果
func (field *Field) CalExp(exp *statement.SingleExpression) (*CalFieldResult, error) {
	var v interface{}
	result := &CalFieldResult{}
	switch exp.CompareOp {
	case "<":
		result.left = 0
		v = field.String2Value(exp.Value)
		result.right = field.Value2Uid(v)
		if result.right > 0 {
			result.right -= 1
		}
		break
	case "=":
		v = field.String2Value(exp.Value)
		result.left = field.Value2Uid(v)
		result.right = result.left
		break
	case ">":
		result.right = math.MaxInt64
		v = field.String2Value(exp.Value)
		result.left = field.Value2Uid(v) + 1
		break
	}

	return result, nil
}

// ParseFieldResult 用于从原始字节数组中解析字段信息
type ParseFieldResult struct {
	v interface{}
	// 下一个值的位置偏移
	shift int
}

// ParseValue 从原始字节数组中解析字段值
func (field *Field) ParseValue(raw []byte) ParseFieldResult {
	var v interface{}
	shift := 0
	switch field.FieldType {
	case "string":
		parseStringResult := commons.ParseString(raw)
		v = parseStringResult.Str
		shift = int(parseStringResult.Next)
		break
	case "int32":
		v = int32(binary.BigEndian.Uint32(raw[:4]))
		shift = 4
		break
	case "int64":
		v = int64(binary.BigEndian.Uint64(raw[:8]))
		shift = 8
		break
	}
	return ParseFieldResult{
		v:     v,
		shift: shift,
	}
}

// PrintValue 打印字段值，主要用于给用户返回查询结果
func (field *Field) PrintValue(v interface{}) string {
	switch field.FieldType {
	case "string":
		return v.(string)
	case "int32":
		return strconv.Itoa(int(v.(int32)))
	case "int64":
		return strconv.Itoa(int(v.(int64)))
	}
	return ""
}

func (field *Field) String() string {
	result := "("
	result += field.FieldName
	result += ", "
	result += field.FieldType
	if field.index != 0 {
		result += ", Index"
	} else {
		result += ", NoIndex"
	}
	result += ")"
	return result
}
