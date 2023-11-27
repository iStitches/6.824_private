package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"os"
	"plugin"

	"6.5840/mr"
	"6.5840/mylog"
)

// func main() {
// 	if len(os.Args) != 2 {
// 		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
// 		os.Exit(1)
// 	}
// 	mapf, reducef := loadPlugin1(os.Args[1])
// 	workerNum, err := strconv.Atoi(os.Args[2])
// 	if err != nil {
// 		panic("Worker start failed, need ")
// 	}
// 	var wg sync.WaitGroup
// 	wg.Add(workerNum)

// 	for i := 0; i < workerNum; i++ {
// 		go mr.Worker(mapf, reducef, i, &wg)
// 	}
// 	wg.Wait()
// }

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
	mapf, reducef := loadPlugin1(os.Args[1])
	// logName := "./worker_" + time.Now().Format("2006-01-02 15:04:05") + ".log"
	// file, err := os.OpenFile(logName, os.O_RDWR|os.O_CREATE, 0644)
	// if err != nil {
	// 	panic("log open failed")
	// }
	mylog.InitLog()
	mr.LOG = mylog.GetLogger()
	// mr.LOG = mylog.Default()
	mr.Worker(mapf, reducef, 1)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin1(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
