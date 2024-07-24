package commons

import (
	"github.com/sirupsen/logrus"
	"os"
)

var (
	// Logger 是全局日志记录器实例
	Logger *logrus.Logger
)

func init() {
	Logger = logrus.New()
	Logger.SetOutput(os.Stdout)
	//Logger.SetLevel(logrus.DebugLevel) // 可以根据需要设置日志级别
	Logger.SetLevel(logrus.InfoLevel) // 可以根据需要设置日志级别
	Logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
}
