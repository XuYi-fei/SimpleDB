package parser

import (
	"dbofmine/backend/parser/statement"
	"dbofmine/commons"
	"errors"
)

func Parse(statement []byte) (interface{}, error) {
	tokenizer := NewTokenizer(statement)
	token, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	tokenizer.Pop()

	var stat interface{} = nil
	// 如果在解析过程中出现错误，保存错误信息
	var statErr error = nil

	switch token {
	case "begin":
		stat, statErr = parseBegin(tokenizer)
		break
	case "commit":
		stat, statErr = parseCommit(tokenizer)
		break
	case "abort":
		stat, statErr = parseAbort(tokenizer)
		break
	case "create":
		stat, statErr = parseCreate(tokenizer)
		break
	case "drop":
		stat, statErr = parseDrop(tokenizer)
		break
	case "select":
		stat, statErr = parseSelect(tokenizer)
		break
	case "insert":
		stat, statErr = parseInsert(tokenizer)
		break
	case "delete":
		stat, statErr = parserDelete(tokenizer)
		break
	case "update":
		stat, statErr = parseUpdate(tokenizer)
		break
	case "show":
		stat, statErr = parseShow(tokenizer)
		break
	default:
		// 如果标记的值不符合预期，抛出异常
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}

	// 获取下一个标记
	next, err := tokenizer.Peek()
	// 如果还有未处理的标记，那么抛出异常
	if next != "" {
		errStat := tokenizer.ErrStat()
		statErr = errors.New("Invalid statement: " + string(errStat))
	}

	if err != nil {
		errStat := tokenizer.ErrStat()
		statErr = errors.New("Invalid statement: " + string(errStat))
	}
	// 如果存在错误，抛出异常
	if statErr != nil {
		return nil, statErr
	}
	// 返回生成的语句对象
	return stat, nil

}

func parseBegin(tokenizer *Tokenizer) (*statement.BeginStatement, error) {
	isolation, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	begin := &statement.BeginStatement{}

	if isolation == "" {
		return begin, nil
	}
	// 获取isolation关键字
	if isolation != "isolation" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取level关键字
	level, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if level != "level" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取等级
	tmp1, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp1 == "read" {
		tokenizer.Pop()
		tmp2, err := tokenizer.Peek()
		if err != nil {
			return nil, err
		}
		if tmp2 == "committed" {
			tokenizer.Pop()
			tmp3, err := tokenizer.Peek()
			if err != nil {
				return nil, err
			}
			if tmp3 != "" {
				return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
			}
			return begin, nil
		} else {
			return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
		}
	} else if tmp1 == "repeatable" {
		tokenizer.Pop()
		tmp2, err := tokenizer.Peek()
		if err != nil {
			return nil, err
		}
		if tmp2 == "read" {
			begin.IsRepeatableRead = true
			tokenizer.Pop()
			tmp3, err := tokenizer.Peek()
			if err != nil {
				return nil, err
			}
			if tmp3 != "" {
				return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
			}
			return begin, nil
		} else {
			return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
		}
	} else {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
}

func parseAbort(tokenizer *Tokenizer) (*statement.AbortStatement, error) {
	tmp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	return &statement.AbortStatement{}, nil
}

func parseCreate(tokenizer *Tokenizer) (*statement.CreateStatement, error) {
	create := &statement.CreateStatement{}
	// 获取table关键字
	tmp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "table" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取表名
	tableName, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if !isName(tableName) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	create.TableName = tableName

	fNames := make([]string, 0)
	fTypes := make([]string, 0)
	for {
		tokenizer.Pop()
		// 获取字段名
		fieldName, err := tokenizer.Peek()
		if err != nil {
			return nil, err
		}
		if fieldName == "(" {
			break
		}
		if !isName(fieldName) {
			return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
		}
		tokenizer.Pop()

		// 获取字段类型
		fieldType, err := tokenizer.Peek()
		if err != nil {
			return nil, err
		}
		if !isType(fieldType) {
			return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
		}
		fNames = append(fNames, fieldName)
		fTypes = append(fTypes, fieldType)
		tokenizer.Pop()

		next, err := tokenizer.Peek()
		if err != nil {
			return nil, err
		}
		if next == "," {
			continue
		} else if next == "" {
			return nil, errors.New(commons.ErrorMessage.TableNoIndexError)
		} else if next == "(" {
			break
		} else {
			return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
		}
	}

	create.FieldName = fNames
	create.FieldType = fTypes

	tokenizer.Pop()
	// 获取index关键字
	tmp, err = tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "index" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}

	// 获取索引
	indexes := make([]string, 0)
	for {
		tokenizer.Pop()
		// 获取索引名
		indexName, err := tokenizer.Peek()
		if err != nil {
			return nil, err
		}
		if indexName == ")" {
			break
		}
		if !isName(indexName) {
			return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
		}
		indexes = append(indexes, indexName)
	}

	create.Index = indexes
	tokenizer.Pop()

	tmp, err = tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	return create, nil
}

func parseCommit(tokenizer *Tokenizer) (*statement.CommitStatement, error) {
	tmp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	return &statement.CommitStatement{}, nil
}

func parserDelete(tokenizer *Tokenizer) (*statement.DeleteStatement, error) {
	deleteStatement := &statement.DeleteStatement{}

	// 获取from关键字
	if tmp, err := tokenizer.Peek(); err != nil || tmp != "from" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取表名
	tableName, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if !isName(tableName) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	deleteStatement.TableName = tableName
	tokenizer.Pop()

	// 获取where子句
	whereStatement, err := parserWhere(tokenizer)
	if err != nil {
		return nil, err
	}
	deleteStatement.Where = whereStatement
	return deleteStatement, nil
}

func parseDrop(tokenizer *Tokenizer) (*statement.DropStatement, error) {
	drop := &statement.DropStatement{}

	tableKey, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tableKey != "table" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取表名
	tableName, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if !isName(tableName) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	drop.TableName = tableName
	tokenizer.Pop()

	tmp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	return drop, nil
}

func parseShow(tokenizer *Tokenizer) (*statement.ShowStatement, error) {
	tmp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		return &statement.ShowStatement{}, nil
	}
	return nil, errors.New(commons.ErrorMessage.InvalidCommandError)

}

func parseUpdate(tokenizer *Tokenizer) (*statement.UpdateStatement, error) {
	update := &statement.UpdateStatement{}
	// 获取表名
	tableName, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	update.TableName = tableName
	tokenizer.Pop()

	// 获取SET关键字
	if tmp, err := tokenizer.Peek(); err != nil || tmp != "set" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取字段名
	fieldName, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	update.FieldName = fieldName
	tokenizer.Pop()

	// 获取等号
	if tmp, err := tokenizer.Peek(); err != nil || tmp != "=" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取字段值
	fieldValue, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	update.Value = fieldValue
	tokenizer.Pop()

	// 获取WHERE子句
	tmp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		update.Where = nil
		return update, nil
	}

	whereStatement, err := parserWhere(tokenizer)
	if err != nil {
		return nil, err
	}
	update.Where = whereStatement
	return update, nil
}

func parseInsert(tokenizer *Tokenizer) (*statement.InsertStatement, error) {
	insert := &statement.InsertStatement{}

	// 获取into关键字
	if tmp, err := tokenizer.Peek(); err != nil || tmp != "into" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取表名
	tableName, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if !isName(tableName) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	insert.TableName = tableName
	tokenizer.Pop()

	// 获取values关键字
	if tmp, err := tokenizer.Peek(); err != nil || tmp != "values" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}

	// 获取values的值
	values := make([]string, 0)
	for {
		tokenizer.Pop()
		value, err := tokenizer.Peek()
		if err != nil {
			return nil, err
		}
		if value == "" {
			break
		} else {
			values = append(values, value)
		}
	}
	insert.Values = values
	return insert, nil
}

func parseSelect(tokenizer *Tokenizer) (*statement.SelectStatement, error) {
	read := &statement.SelectStatement{}

	fields := make([]string, 0)
	asterisk, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	// 如果是*，那么获取所有字段
	if asterisk == "*" {
		fields = append(fields, "*")
		tokenizer.Pop()
	} else {
		// 否则获取字段名
		for {
			field, err := tokenizer.Peek()
			if err != nil {
				return nil, err
			}
			if !isName(field) {
				return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
			}
			fields = append(fields, field)
			tokenizer.Pop()
			tmp, err := tokenizer.Peek()
			if err != nil {
				return nil, err
			}
			if tmp == "," {
				tokenizer.Pop()
			} else {
				break
			}
		}
	}

	read.Fields = fields

	// 获取from关键字
	from, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if from != "from" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取表名
	tableName, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if !isName(tableName) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	read.TableName = tableName
	tokenizer.Pop()

	// 获取where子句
	where, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if where == "" {
		read.Where = nil
		return read, nil
	}

	whereStatement, err := parserWhere(tokenizer)
	if err != nil {
		return nil, err
	}
	read.Where = whereStatement
	return read, nil
}

func parserWhere(tokenizer *Tokenizer) (*statement.WhereSubStatement, error) {
	where := &statement.WhereSubStatement{}

	// 获取where关键字
	tmp, err := tokenizer.Peek()
	if err != nil || tmp != "where" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	tokenizer.Pop()

	// 获取第一个表达式
	singleExp1, err := parseSingleExpression(tokenizer)
	if err != nil {
		return nil, err
	}
	where.SingleExp1 = singleExp1

	logicOp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if logicOp == "" {
		where.LogicOp = logicOp
		return where, nil
	}
	if !isLogicOp(logicOp) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	where.LogicOp = logicOp
	tokenizer.Pop()

	// 获取第二个表达式
	singleExp2, err := parseSingleExpression(tokenizer)
	if err != nil {
		return nil, err
	}
	where.SingleExp2 = singleExp2

	tmp, err = tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "" {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}

	return where, nil

}

func parseSingleExpression(tokenizer *Tokenizer) (*statement.SingleExpression, error) {
	exp := &statement.SingleExpression{}

	// 获取字段名
	field, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if !isName(field) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	exp.Field = field
	tokenizer.Pop()

	// 获取比较运算符
	compareOp, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	if !isCmpOp(compareOp) {
		return nil, errors.New(commons.ErrorMessage.InvalidCommandError)
	}
	exp.CompareOp = compareOp
	tokenizer.Pop()

	// 获取值
	value, err := tokenizer.Peek()
	if err != nil {
		return nil, err
	}
	exp.Value = value
	tokenizer.Pop()

	return exp, nil
}

// TODO 拓展比较运算符
func isCmpOp(op string) bool {
	return op == "=" || op == ">" || op == "<"
}

func isLogicOp(op string) bool {
	return op == "and" || op == "or"
}

func isType(tp string) bool {
	return tp == "int32" || tp == "string" || tp == "int64"
}

func isName(name string) bool {
	return !(len(name) == 1 && !IsAlphaBeta(name[0]))
}
