package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.5840/mr"
	"6.5840/mylog"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	logName := "./coordinator_" + time.Now().Format("2006-01-02 15:04:05") + ".log"
	file, err := os.OpenFile(logName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic("open log file failed")
	}
	mr.LOG = mylog.New(file, mylog.DebugLevel)
	// mr.LOG = mylog.Default()
	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
