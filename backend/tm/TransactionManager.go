package tm

// TransactionManager interface
type TransactionManager interface {
	Begin() int64

	Commit(xid int64)

	Abort(xid int64)

	IsActive(xid int64) bool

	IsCommitted(xid int64) bool

	IsAborted(xid int64) bool

	Close()
}
