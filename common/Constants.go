package common

// ErrorMessageType 错误信息常量
type ErrorMessageType struct {
	// 文件已存在
	FileExistError string
	// 写入文件头错误
	WriteFileHeaderError string
	//
	BadXIDFileException string
}

var ErrorMessage = ErrorMessageType{
	FileExistError:       "文件已存在",
	WriteFileHeaderError: "写入文件头错误",
	BadXIDFileException:  "Bad XID file!",
}
