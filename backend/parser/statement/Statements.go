package statement

type AbortStatement struct {
}

type BeginStatement struct {
	IsRepeatableRead bool
}

type CommitStatement struct {
}

type CreateStatement struct {
	TableName string
	FieldName []string
	FieldType []string
	Index     []string
}

type DeleteStatement struct {
	TableName string
	Where     *WhereSubStatement
}

type DropStatement struct {
	TableName string
}

type InsertStatement struct {
	TableName string
	Values    []string
}

type SelectStatement struct {
	TableName string
	Fields    []string
	Where     *WhereSubStatement
}

type ShowStatement struct {
}

type UpdateStatement struct {
	TableName string
	FieldName string
	Value     string
	Where     *WhereSubStatement
}

type WhereSubStatement struct {
	SingleExp1 *SingleExpression
	LogicOp    string
	SingleExp2 *SingleExpression
}

// ============ 用于表达查询条件的结构体 ============

type SingleExpression struct {
	Field     string
	CompareOp string
	Value     string
}
