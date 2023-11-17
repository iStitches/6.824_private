package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//*****************************************Worker Function****************************************//
// Worker 启动时执行
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	workerId int) {
	isAlive := true
	taskCount := 0
	for isAlive {
		taskCount++
		job := CallForJob(workerId)
		if job != nil {
			switch job.Type {
			case RPC_JOBTYPE_MAP:
				if err := executeMapTask(mapf, job); err != nil {
					LOG.Error("Worker Crashed:: executeMapTask failed",
						zap.Int("JobId", job.Id))
				} else {
					CallForDone(job)
				}
			case RPC_JOBTYPE_REDUCE:
				if err := executeReduceTask(reducef, job); err != nil {
					LOG.Error("Worker Crashed:: executeReduceTask failed",
						zap.Int("JobId", job.Id))
				} else {
					CallForDone(job)
				}
			case RPC_JOBTYPE_TRANSER:
				// when coordinator transitions from MAPSTATUS to REDUCESTATUS
				time.Sleep(time.Second)
			case RPC_JOBTYPE_END:
				isAlive = false
			}
		}
		time.Sleep(time.Second)
	}
	LOG.Info("Worker end....",
		zap.Int("WorkerId", workerId))
}

//
// 执行 Map 任务
//
func executeMapTask(mapf func(string, string) []KeyValue, job *Job) error {
	fileName := job.FileName[0]
	context, err := ioutil.ReadFile(fileName)
	if err != nil {
		LOG.Error("ExecuteMapTask:: open file failed")
	}
	mapResult := mapf(fileName, string(context))

	// divide mapResult into reduceNum parts
	reduceNum := job.ReduceNumber
	tempReduces := make([][]KeyValue, reduceNum)
	for i, kv := range mapResult {
		idx := ihash(kv.Key) % reduceNum
		tempReduces[idx] = append(tempReduces[idx], mapResult[i])
	}
	// store groupResult into intermediate file
	dir, _ := os.Getwd()
	for i := 0; i < reduceNum; i++ {
		file, err := ioutil.TempFile(dir, "mr-intermediate-*")
		if err != nil {
			LOG.Error("ExecuteMapTask:: create intermediate-file failed")
			return err
		}
		enc := json.NewEncoder(file)
		for _, kv := range tempReduces[i] {
			enc.Encode(kv)
		}
		file.Close()
		// rename intermediate file
		convFileName := "mr-" + strconv.Itoa(job.Id) + "-" + strconv.Itoa(i)
		os.Rename(file.Name(), convFileName)
	}
	return nil
}

//
// 执行 Reduce 任务
//
func executeReduceTask(reducef func(string, []string) string, job *Job) error {
	// read all file contents
	files := job.FileName
	fileContent := make([]KeyValue, 0)
	for _, name := range files {
		file, err := os.Open(name)
		if err != nil {
			LOG.Error("executeReduceTask:: can't open reduceFile")
			return err
		}
		dec := json.NewDecoder(file)
		for {
			vk := KeyValue{}
			if err := dec.Decode(&vk); err != nil {
				break
			}
			fileContent = append(fileContent, vk)
		}
	}

	// sort all key
	sort.Sort(ByKey(fileContent))
	// call reducef for values
	dir, _ := os.Getwd()
	out, _ := ioutil.TempFile(dir, "mr-out-*")

	i := 0
	values := make([]string, 0)
	for i < len(fileContent) {
		values = values[:0]
		j := i + 1
		for j < len(fileContent) && fileContent[i].Key == fileContent[j].Key {
			j++
		}
		for k := i; k < j; k++ {
			values = append(values, fileContent[k].Value)
		}
		count := reducef(fileContent[i].Key, values)
		fmt.Fprintf(out, "%v %v\n", fileContent[i].Key, count)
		i = j
	}
	out.Close()

	// rename intermediate file
	cutIdx := strings.LastIndex(out.Name(), "-")
	reNameFile := out.Name()[0:cutIdx+1] + strconv.Itoa(job.Id)
	if err := os.Rename(out.Name(), reNameFile); err != nil {
		LOG.Error("executeReduceTask:: can't rename reduceFile")
		return err
	}
	return nil
}

func ValidJobType(job *Job) bool {
	if job.Type == RPC_JOBTYPE_MAP || job.Type == RPC_JOBTYPE_REDUCE {
		return true
	}
	return false
}

//*****************************************RPC Client****************************************//
// 获取任务 RPC
//
func CallForJob(workerId int) *Job {
	args := ExampleArgs{}
	reply := Job{}
	ok := call(RPC_DISTRIBUTE_JOB, &args, &reply)
	if !ok {
		LOG.Error("Worker get job failed",
			zap.Int("workerId", workerId))
	} else {
		if ValidJobType(&reply) {
			LOG.Info("Worker get job success",
				zap.Int("workerId", workerId),
				zap.Int("jobId", reply.Id),
				zap.Int("jobType", reply.Type))
		}
	}
	return &reply
}

//
// 任务执行结束 RPC
//
func CallForDone(j *Job) {
	reply := ExampleReply{}
	ok := call(RPC_FINISH_JOB, j, &reply)
	if !ok {
		LOG.Error("Info Coordinator job finishing failed",
			zap.Int("jobId", j.Id))
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
