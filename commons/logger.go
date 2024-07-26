package commons

import (
	"github.com/sirupsen/logrus"
	"os"
	"runtime"
	"strings"
)

var (
	// Logger 是全局日志记录器实例
	Logger      *logrus.Logger
	LoggerLevel = logrus.DebugLevel
	// ProjectRoot 项目根目录
	ProjectRoot = "dbofmine"
)

// ContextHook 定义一个结构体实现 logrus.Hook 接口
type ContextHook struct{}

// Levels 返回 Hook 应用的日志级别
func (hook ContextHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire 是 Hook 的核心方法，在每次日志触发时调用
func (hook ContextHook) Fire(entry *logrus.Entry) error {
	pc, file, line, ok := runtime.Caller(8) // 调用栈层次
	if !ok {
		return nil
	}

	// 截断文件路径，只保留从项目根目录开始的部分
	if index := strings.Index(file, ProjectRoot); index != -1 {
		file = file[index+len(ProjectRoot):]
		if file[0] == '/' || file[0] == '\\' {
			file = file[1:]
		}
	}

	funcName := runtime.FuncForPC(pc).Name()

	// 截断文件路径，只保留从项目根目录开始的部分
	if index := strings.Index(funcName, ProjectRoot); index != -1 {
		funcName = funcName[index+len(ProjectRoot):]
		if funcName[0] == '/' || funcName[0] == '\\' {
			funcName = funcName[1:]
		}
	}
	//entry.Data["file"] = file
	entry.Data["line"] = line
	entry.Data["func"] = funcName
	return nil
}

// NewLogger 创建一个新的 Logger 实例
func NewLogger() *logrus.Logger {
	logger := logrus.New()
	initLogger(logger)
	return logger
}

// NewLoggerByLevel 创建一个新的 Logger 实例，并设置日志级别
func NewLoggerByLevel(level logrus.Level) *logrus.Logger {
	logger := logrus.New()
	initLogger(logger)
	logger.SetLevel(level)
	return logger
}

// initLogger 初始化 Logger 实例
func initLogger(logger *logrus.Logger) {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(LoggerLevel) // 可以根据需要设置日志级别
	// 设置日志格式
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})
	// 添加自定义 Hook
	logger.AddHook(ContextHook{})
}

// init 初始化全局 Logger 实例
func init() {
	Logger = logrus.New()
	initLogger(Logger)
}
