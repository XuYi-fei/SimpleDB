package tbm

import (
	"SimpleDB/backend/parser/statement"
	"SimpleDB/backend/tm"
	"SimpleDB/commons"
	"encoding/binary"
	"errors"
	"math"
)

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */

type Table struct {
	// 表管理器，用于管理数据库表
	TBM *TableManager
	// 表的唯一标识符
	Uid int64
	// 表的名称
	Name string
	// 表的状态
	status byte
	// 下一个表的唯一标识符
	NextUid int64
	// 表的字段列表
	Fields []*Field
}

// CreateTable 创建一个新的数据库表
func CreateTable(tbm *TableManager, nextUid int64, xid int64, create *statement.CreateStatement) (*Table, error) {
	// 创建一个新的表对象
	table := &Table{
		TBM:     tbm,
		Name:    create.TableName,
		NextUid: nextUid,
	}
	// 遍历创建表语句中的所有字段
	for i := 0; i < len(create.FieldName); i++ {
		// 获取字段名和字段类型
		fieldName := create.FieldName[i]
		fieldType := create.FieldType[i]
		// 判断该字段是否需要建立索引
		indexed := false
		for _, index := range create.Index {
			if index == fieldName {
				indexed = true
				break
			}
		}
		// 创建一个新的字段对象
		newField, err := CreateField(table, xid, fieldName, fieldType, indexed)
		if err != nil {
			return nil, err
		}
		table.Fields = append(table.Fields, newField)
	}
	// 将表对象的状态持久化到存储系统中，并返回表对象
	return table.persistSelf(xid)
}

// LoadTable 用于从数据库中加载一个表
func LoadTable(tbm *TableManager, uid int64) *Table {
	// 初始化一个字节数组用于存储从数据库中读取的原始数据
	var raw []byte
	// 使用表管理器的版本管理器从数据库中读取指定uid的表的原始数据
	raw, err := tbm.VM.Read(tm.SuperXid, uid)
	if err != nil {
		// 如果在读取过程中发生异常，处理异常
		panic(err)
	}

	// 断言原始数据不为空
	if raw == nil {
		// 如果原始数据为空，抛出异常
		panic("原始字节数据不为nil，如果为nil，那么会抛出AssertionError")
	}
	// 创建一个新的表对象
	table := &Table{
		TBM: tbm,
		Uid: uid,
	}
	// 使用原始数据解析表对象，并返回这个表对象
	return table.parseSelf(raw)
}

// parseSelf 用于解析表对象
func (table *Table) parseSelf(raw []byte) *Table {
	// 初始化位置变量
	pos := 0
	// 解析原始数据中的字符串
	parseStringResult := commons.ParseString(raw)
	// 将解析出的字符串赋值给表的名称
	table.Name = parseStringResult.Str
	// 更新位置变量
	pos += int(parseStringResult.Next)

	// 解析原始数据中的长整数，并赋值给下一个uid
	table.NextUid = int64(binary.BigEndian.Uint64(raw[pos : pos+8]))
	// 更新位置变量
	pos += 8

	// 当位置变量小于原始数据的长度时，继续循环
	for pos < len(raw) {
		// 解析原始数据中的长整数，并赋值给uid
		uid := int64(binary.BigEndian.Uint64(raw[pos : pos+8]))
		// 更新位置变量
		pos += 8
		// 使用Field.loadField方法加载字段，并添加到表的字段列表中
		table.Fields = append(table.Fields, LoadField(table, uid))
	}
	return table
}

// persistSelf 将Table对象的状态持久化到存储系统中
func (table *Table) persistSelf(xid int64) (*Table, error) {
	// 将表名转换为字节数组
	nameBytes := commons.String2Bytes(table.Name)
	// 将下一个uid转换为字节数组
	nextUidBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nextUidBytes, uint64(table.NextUid))
	// 创建一个空的字节数组，用于存储字段的uid
	fieldRaw := make([]byte, 0)

	// 遍历所有的字段
	for _, field := range table.Fields {
		// 将字段的uid转换为字节数组，并添加到fieldRaw中
		fieldUidBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(fieldUidBytes, uint64(field.Uid))

		fieldRaw = append(fieldRaw, fieldUidBytes...)
	}

	// 将表名、下一个uid和所有字段的uid插入到存储系统中，返回插入的uid
	data := commons.BytesConcat(nameBytes, nextUidBytes, fieldRaw)
	uid, err := table.TBM.VM.Insert(xid, data)
	if err != nil {
		return nil, err
	}
	table.Uid = uid

	// 将表对象的状态持久化到存储系统中
	return table, nil
}

// CalWhereResult 用来保存where查询后的结果
type CalWhereResult struct {
	l0     int64
	r0     int64
	l1     int64
	r1     int64
	single bool
}

// parseWhere 解析 WHERE 子句并返回满足条件的记录的 uid 列表
func (table *Table) parseWhere(where *statement.WhereSubStatement) ([]int64, error) {
	// 初始化搜索范围和标志位
	var l0 int64 = 0
	var r0 int64 = 0
	var l1 int64 = 0
	var r1 int64 = 0
	// 用来标记是否只有一个查询条件(即是否不包含or and等)
	single := false
	var fd *Field

	// 如果 WHERE 子句为空，则搜索所有记录
	if where == nil {
		// 寻找第一个有索引的字段
		for _, field := range table.Fields {
			if field.IsIndexed() {
				fd = field
				break
			}
		}
		// 设置搜索范围为整个 uid 空间
		l0, r0 = 0, math.MaxInt64
		single = true
	} else {
		// 如果 WHERE 子句不为空，则根据 WHERE 子句解析搜索范围
		// 寻找 WHERE 子句中涉及的字段
		for _, field := range table.Fields {
			if field.FieldName == where.SingleExp1.Field {
				// 如果字段没有索引，则抛出异常
				if !field.IsIndexed() {
					return nil, errors.New(commons.ErrorMessage.FieldNotIndexedError)
				}
				fd = field
				break
			}
		}
		// 如果字段不存在，则抛出异常
		if fd == nil {
			return nil, errors.New(commons.ErrorMessage.FieldNotFoundError)
		}
		// 计算 WHERE 子句的搜索范围
		calWhereResult, err := table.calWhere(fd, where)
		if err != nil {
			return nil, err
		}
		l0, r0 = calWhereResult.l0, calWhereResult.r0
		l1, r1 = calWhereResult.l1, calWhereResult.r1
		single = calWhereResult.single
	}
	// 在计算出的搜索范围内搜索记录
	uids, err := fd.Search(l0, r0)
	if err != nil {
		return nil, err
	}
	// 如果 WHERE 子句包含 OR 运算符，则需要搜索两个范围，并将结果合并
	if !single {
		uids1, err := fd.Search(l1, r1)
		if err != nil {
			return nil, err
		}
		uids = append(uids, uids1...)
	}
	return uids, nil
}

func (table *Table) calWhere(fd *Field, where *statement.WhereSubStatement) (*CalWhereResult, error) {
	result := &CalWhereResult{}
	switch where.LogicOp {
	case "":
		result.single = true
		// 如果没有逻辑运算符，则直接计算搜索范围
		fieldCalResult, err := fd.CalExp(where.SingleExp1)
		if err != nil {
			return nil, err
		}
		result.l0, result.r0 = fieldCalResult.left, fieldCalResult.right
		break
	case "or":
		result.single = false
		// 如果逻辑运算符为 or，则计算两个子表达式的搜索范围
		fieldCalResult1, err := fd.CalExp(where.SingleExp1)
		if err != nil {
			return nil, err
		}
		fieldCalResult2, err := fd.CalExp(where.SingleExp2)
		if err != nil {
			return nil, err
		}
		result.l0 = fieldCalResult1.left
		result.r0 = fieldCalResult1.right
		result.l1 = fieldCalResult2.left
		result.r1 = fieldCalResult2.right
		break
	case "and":
		result.single = true
		// 如果逻辑运算符为 and，则计算两个子表达式的交集
		fieldCalResult1, err := fd.CalExp(where.SingleExp1)
		if err != nil {
			return nil, err
		}
		fieldCalResult2, err := fd.CalExp(where.SingleExp2)
		if err != nil {
			return nil, err
		}
		result.l0, result.r0 = fieldCalResult1.left, fieldCalResult1.right
		result.l1, result.r1 = fieldCalResult2.left, fieldCalResult2.right
		if result.l0 < result.l1 {
			result.l0 = result.l1
		}
		if result.r0 > result.r1 {
			result.r0 = result.r1
		}
		break
	default:
		return nil, errors.New(commons.ErrorMessage.InvalidLogOpError)
	}
	return result, nil
}

// =========== 如下处理各个语句 ===========

func (table *Table) Delete(xid int64, delete *statement.DeleteStatement) (int, error) {
	// 解析 WHERE 子句
	uids, err := table.parseWhere(delete.Where)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, uid := range uids {
		// 删除记录
		deleted, err := table.TBM.VM.Delete(xid, uid)
		if err != nil {
			return 0, err
		}
		if deleted {
			count++
		}
	}
	return count, nil
}

func (table *Table) Update(xid int64, update *statement.UpdateStatement) (int, error) {
	// 解析 WHERE 子句
	uids, err := table.parseWhere(update.Where)
	if err != nil {
		return 0, err
	}
	var fd *Field
	for _, field := range table.Fields {
		if field.FieldName == update.FieldName {
			fd = field
			break
		}
	}
	if fd == nil {
		return 0, errors.New(commons.ErrorMessage.FieldNotFoundError)
	}

	value := fd.String2Value(update.Value)
	// 成功更新记录的数目
	count := 0
	for _, uid := range uids {
		raw, err := table.TBM.VM.Read(xid, uid)
		if err != nil {
			return 0, err
		}
		if raw == nil {
			continue
		}

		// 先删除旧记录（更新XMax）
		_, err = table.TBM.VM.Delete(xid, uid)
		if err != nil {
			return 0, err
		}

		// 先取出来这一条表记录
		entry := table.parseEntry(raw)
		// 再插入新记录
		entry[update.FieldName] = value
		updatedRaw := table.entry2Raw(entry)
		uuid, err := table.TBM.VM.Insert(xid, updatedRaw)
		if err != nil {
			return 0, err
		}

		// 更新记录，记录更新成功的数目
		count++

		for _, field := range table.Fields {
			if field.IsIndexed() {
				err = field.Insert(value, uuid)
				if err != nil {
					return 0, err
				}
			}

		}

	}
	return count, nil
}

// Read 用于读取表中的记录，返回查询结果
func (table *Table) Read(xid int64, read *statement.SelectStatement) (string, error) {
	uids, err := table.parseWhere(read.Where)
	if err != nil {
		return "", err
	}
	result := ""

	for _, uid := range uids {
		raw, err := table.TBM.VM.Read(xid, uid)
		if err != nil {
			return "", err
		}
		if raw == nil {
			continue
		}
		entry := table.parseEntry(raw)
		result += table.printEntry(entry)
		result += "\n"
	}

	return result, nil
}

// Insert 用于向表中插入记录
func (table *Table) Insert(xid int64, insert *statement.InsertStatement) error {
	entry, err := table.string2Entry(insert.Values)
	if err != nil {
		return err
	}
	raw := table.entry2Raw(entry)
	uid, err := table.TBM.VM.Insert(xid, raw)
	if err != nil {
		return err
	}
	for _, field := range table.Fields {
		if field.IsIndexed() {
			err = field.Insert(entry[field.FieldName], uid)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// =========== 如下进行字段中entry和原始字节的转换，用于读取和存储具体的字段中的值 ===========

func (table *Table) string2Entry(values []string) (map[string]interface{}, error) {
	if len(values) != len(table.Fields) {
		return nil, errors.New(commons.ErrorMessage.InvalidValuesError)
	}
	entry := make(map[string]interface{})
	for i, _ := range values {
		field := table.Fields[i]
		v := field.String2Value(values[i])
		entry[field.FieldName] = v
	}
	return entry, nil

}

func (table *Table) printEntry(entry map[string]interface{}) string {
	var str string = "["
	for i, _ := range table.Fields {
		field := table.Fields[i]
		str += field.PrintValue(entry[field.FieldName])
		if i == len(table.Fields)-1 {
			str += "]"
		} else {
			str += ","
		}
	}
	return str
}

// parseEntry 用于解析原始字节数据并返回一个Entry对象
func (table *Table) parseEntry(raw []byte) map[string]interface{} {
	pos := 0
	entry := make(map[string]interface{})
	for _, field := range table.Fields {
		parseValueResult := field.ParseValue(raw[pos:])
		entry[field.FieldName] = parseValueResult.v
		pos += parseValueResult.shift
	}
	return entry
}

// entry2Raw 用于将Entry对象转换为原始字节数据
func (table *Table) entry2Raw(entry map[string]interface{}) []byte {
	raw := make([]byte, 0)
	for _, field := range table.Fields {
		raw = append(raw, field.Value2Raw(entry[field.FieldName])...)
	}
	return raw
}

func (table *Table) String() string {
	result := "{"
	result += table.Name
	for i, field := range table.Fields {
		result += field.String()
		if i == len(table.Fields)-1 {
			result += "}"
		} else {
			result += ", "
		}
	}
	return result
}
