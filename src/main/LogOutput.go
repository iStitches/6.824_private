package main

import (
	"6.5840/mylog"
	"go.uber.org/zap"
)

func main() {
	mylog.InitLog()
	zlog := mylog.GetLogger()
	zlog.Info("aaaa", zap.Int("PeerId", 1), zap.String("Msg", "aaaaa"))
}
