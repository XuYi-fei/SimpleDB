package server

import (
	"SimpleDB/backend/parser"
	"SimpleDB/backend/parser/statement"
	"SimpleDB/backend/tbm"
	"SimpleDB/commons"
	"errors"
)

type Executor struct {
	xid int64
	TBM *tbm.TableManager
}

func NewExecutor(tbm *tbm.TableManager) *Executor {
	return &Executor{
		xid: 0,
		TBM: tbm,
	}
}

func (e *Executor) Close() {
	if e.xid != 0 {
		commons.Logger.Warnf("Abnormal Abort: %d", e.xid)
		e.TBM.Abort(e.xid)
	}
}

func (e *Executor) Execute(sql []byte) ([]byte, error) {
	commons.Logger.Infof("Execute SQL: %s", string(sql))

	stat, err := parser.Parse(sql)
	if err != nil {
		commons.Logger.Warnf("Parse SQL error: %s", err.Error())
		return nil, err
	}

	switch stat.(type) {
	case *statement.BeginStatement:
		if e.xid != 0 {
			return nil, errors.New(commons.ErrorMessage.NestedTransactionError)
		}
		beginResult := e.TBM.Begin(stat.(*statement.BeginStatement))
		e.xid = beginResult.Xid
		return beginResult.Result, nil
	case *statement.CommitStatement:
		if e.xid == 0 {
			return nil, errors.New(commons.ErrorMessage.NoTransactionError)
		}
		res, err := e.TBM.Commit(e.xid)
		if err != nil {
			return nil, err
		}
		e.xid = 0
		return res, nil
	case *statement.AbortStatement:
		if e.xid == 0 {
			return nil, errors.New(commons.ErrorMessage.NoTransactionError)
		}
		res := e.TBM.Abort(e.xid)
		e.xid = 0
		return res, nil
	default:
		return e.execute2(stat)
	}

}

func (e *Executor) execute2(stat interface{}) ([]byte, error) {
	tmpTransaction := false
	var err error = nil

	// 如果当前没有事务，则开启一个新的事务
	if e.xid == 0 {
		beginResult := e.TBM.Begin(&statement.BeginStatement{})
		e.xid = beginResult.Xid
		tmpTransaction = true
	}

	defer func() {
		if tmpTransaction {
			if err != nil {
				e.TBM.Abort(e.xid)
			} else {
				e.TBM.Commit(e.xid)
			}
			e.xid = 0
		}
	}()

	var result []byte = nil
	switch stat.(type) {
	case *statement.ShowStatement:
		result = e.TBM.Show(e.xid)
		break
	case *statement.CreateStatement:
		result, err = e.TBM.Create(e.xid, stat.(*statement.CreateStatement))
		break
	case *statement.SelectStatement:
		result, err = e.TBM.Read(e.xid, stat.(*statement.SelectStatement))
		break
	case *statement.InsertStatement:
		result, err = e.TBM.Insert(e.xid, stat.(*statement.InsertStatement))
		break
	case *statement.DeleteStatement:
		result, err = e.TBM.Delete(e.xid, stat.(*statement.DeleteStatement))
		break
	case *statement.UpdateStatement:
		result, err = e.TBM.Update(e.xid, stat.(*statement.UpdateStatement))
		break
	}
	if err != nil {
		return nil, err
	}

	return result, nil

}
