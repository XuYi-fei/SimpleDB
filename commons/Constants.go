package commons

// ErrorMessageType 错误信息常量
type ErrorMessageType struct {
	// 文件已存在
	FileExistError string
	// 写入文件头错误
	WriteFileHeaderError string
	// Bad XID file!
	BadXIDFileException string

	// 缓存实现中的错误
	// 缓存已满，需要删除一个资源
	CacheIsFullError string
	// 分配用于缓存的内存过小
	AllocMemoryTooSmallError string

	// 日志文件错误
	BadLogFileError string
	// 日志校验失败错误
	BadLogCheckSumError string

	// Data数据太大错误
	DataTooLargeError string
	// 数据库正忙，无法分配新的页面来存储数据
	DatabaseBusyError string

	// 死锁异常
	DeadLockError string

	// Entry为空异常
	NullEntryError string

	// 并发更新错误
	ConcurrentUpdateError string

	// 语句解析错误
	InvalidCommandError string

	// 表中无索引
	TableNoIndexError string
}

var ErrorMessage = ErrorMessageType{
	FileExistError:           "文件已存在",
	WriteFileHeaderError:     "写入文件头错误",
	BadXIDFileException:      "Bad XID file!",
	CacheIsFullError:         "缓存已满，需要删除一个资源",
	AllocMemoryTooSmallError: "分配用于缓存的内存过小",
	BadLogFileError:          "日志文件错误",
	BadLogCheckSumError:      "日志校验失败错误",
	DataTooLargeError:        "Data too large",
	DatabaseBusyError:        "Database is busy!",
	DeadLockError:            "Deadlock detected",
	NullEntryError:           "Entry is null",
	ConcurrentUpdateError:    "Concurrent update error",
	InvalidCommandError:      "Invalid command",
	TableNoIndexError:        "Table has no index",
}
