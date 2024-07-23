package tm

// TransactionManager interface
type TransactionManager interface {
	begin() int64

	commit(xid int64)

	abort(xid int64)

	isActive(xid int64) bool

	isCommitted(xid int64) bool

	isAborted(xid int64) bool

	close()
}
