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
}

var ErrorMessage = ErrorMessageType{
	FileExistError:       "文件已存在",
	WriteFileHeaderError: "写入文件头错误",
	BadXIDFileException:  "Bad XID file!",
	CacheIsFullError:     "缓存已满，需要删除一个资源",
}
