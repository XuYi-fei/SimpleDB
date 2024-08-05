package tests

import (
	"SimpleDB/backend/parser"
	"SimpleDB/backend/parser/statement"
	"testing"
)

func TestCreate(t *testing.T) {
	t.Log("TestCreate")
	stat := "create table student id int32, name string, uid int64, (index name id uid)"
	res, err := parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	create, ok := res.(*statement.CreateStatement)
	if !ok {
		t.Error("not create statement")
	}
	if create.TableName != "student" {
		t.Error("table name error")
	}

	for i := 0; i < len(create.FieldName); i++ {
		t.Log(create.FieldName[i] + ":" + create.FieldType[i])
	}
	t.Logf("index: %v\n", create.Index)
	t.Log("==================")
}

func TestBegin(t *testing.T) {
	t.Log("TestBegin")
	stat := "begin isolation level read committed"
	res, err := parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	begin, ok := res.(*statement.BeginStatement)
	if !ok {
		t.Error("not begin statement")
	}
	if begin.IsRepeatableRead == true {
		t.Error("level error")
	}
	t.Log(begin)
	t.Log("==================")

	stat = "begin"
	res, err = parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	begin, ok = res.(*statement.BeginStatement)
	if !ok {
		t.Error("not begin statement")
	}
	if begin.IsRepeatableRead == true {
		t.Error("level error")
	}
	t.Log(begin)
	t.Log("==================")

	stat = "begin isolation level repeatable read"
	res, err = parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	begin, ok = res.(*statement.BeginStatement)
	if !ok {
		t.Error("not begin statement")
	}
	if begin.IsRepeatableRead != true {
		t.Error("level error")
	}
	t.Log(begin)
	t.Log("==================")
}

func TestRead(t *testing.T) {
	t.Log("TestRead")
	stat := "select name, id, student from student where id > 1 and id < 4"
	res, err := parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	read, ok := res.(*statement.SelectStatement)
	if !ok {
		t.Error("not read statement")
	}
	if read.TableName != "student" {
		t.Error("table name error")
	}

	t.Logf("where: %v\n", read.Where)
	t.Log(read)
	t.Log("==================")
}

func TestInsert(t *testing.T) {
	t.Log("TestInsert")
	stat := "insert into student values 1, 'zhangsan', 22"
	res, err := parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	insert, ok := res.(*statement.InsertStatement)
	if !ok {
		t.Error("not insert statement")
	}
	if insert.TableName != "student" {
		t.Error("table name error")
	}

	t.Logf("values: %v\n", insert.Values)
	t.Log(insert)
	t.Log("==================")
}

func TestDelete(t *testing.T) {
	t.Log("TestDelete")
	stat := "delete from student where name = \"Xu Yifei\""
	res, err := parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	deleteStatement, ok := res.(*statement.DeleteStatement)
	if !ok {
		t.Error("not deleteStatement statement")
	}
	if deleteStatement.TableName != "student" {
		t.Error("table name error")
	}

	t.Logf("where: %v\n", deleteStatement.Where)
	t.Log(deleteStatement)
	t.Log("==================")
}

func TestShow(t *testing.T) {
	t.Log("TestShow")
	stat := "show"
	res, err := parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	show, ok := res.(*statement.ShowStatement)
	if !ok {
		t.Error("not show statement")
	}
	t.Log(show)
	t.Log("==================")
}

func TestUpdate(t *testing.T) {
	t.Log("TestUpdate")
	stat := "update student set name = \"Xu Yifei\" where id = 1"
	res, err := parser.Parse([]byte(stat))
	if err != nil {
		t.Error(err)
	}

	update, ok := res.(*statement.UpdateStatement)
	if !ok {
		t.Error("not update statement")
	}
	if update.TableName != "student" {
		t.Error("table name error")
	}

	t.Logf("field: %v\n", update.FieldName)
	t.Logf("value: %v\n", update.Value)
	t.Logf("where: %v\n", update.Where)
	t.Log(update)
	t.Log("==================")
}
