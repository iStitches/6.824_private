package mylog

import (
	"sync"

	"go.uber.org/zap"
)

var logger *zap.Logger

var once sync.Once

const (
	level       = "info"
	appName     = "app"
	development = true
	maxSize     = 100
	maxBackups  = 60
	maxAge      = 30
)

// log instance init
func InitLog() {
	logLevel := zap.DebugLevel
	switch level {
	case "debug":
		logLevel = zap.DebugLevel
	case "info":
		logLevel = zap.InfoLevel
	case "error":
		logLevel = zap.ErrorLevel
	case "warn":
		logLevel = zap.WarnLevel
	}

	logger = NewLogger(
		SetAppName(appName),
		SetDevelopment(development),
		SetMaxSize(maxSize),
		SetMaxBackups(maxBackups),
		SetMaxAge(maxAge),
		SetLevel(logLevel),
	)
}

// 获取日志信息
func GetLogger() *zap.Logger {
	return logger
}
