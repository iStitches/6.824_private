package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"6.5840/meta"
	"go.uber.org/zap"
)

type Condition int

var lock sync.Mutex

//*****************************************元数据信息****************************************//
//
// Coordinator 结构体信息
//
type Coordinator struct {
	State            Condition
	MapJobChannel    chan *Job
	ReduceJobChannel chan *Job
	MapJobNums       int
	ReduceJobNums    int
	JobIndex         int
	JobInfoMap       *JobMetaInfoHolder
	WorkerInfoMap    *WorkerMetaInfoHolder
}

// 任务数据信息
type Job struct {
	Id           int      //任务序号
	Type         int      //任务类型 Map/Reduce
	FileName     []string //文件名
	ReduceNumber int      //执行Reduce的 Worker数量.
	Version      int      //版本号，当出现任务重发时区分新旧任务，防止将旧任务结果当作新任务结果
}

// 任务状态信息
type JobMetaInfo struct {
	JobPtr    *Job
	State     Condition //任务状态
	StartTime time.Time //开始执行时间
	EndTime   time.Time //结束时间
}

// 任务状态管理
type JobMetaInfoHolder struct {
	MetaInfoMap map[int]*JobMetaInfo
}

// Worker 状态信息
type WorkerMetaInfo struct {
	Id             int       //机器号
	State          Condition //状态
	Ip             string    //IP地址
	CurrentJob     int       //当前执行任务号
	FinishedJobIds []int     //已完成任务编号集合
}

// Worker状态管理
type WorkerMetaInfoHolder struct {
	MetaInfoMap map[int]*WorkerMetaInfo
}

//*****************************************任务执行处理****************************************//
// 预处理构造Map任务，任务完成后检查切换 Coordinator 状态
//
func (c *Coordinator) CreateMapJobs(files []string) error {
	for index, _ := range files {
		job := Job{
			Id:           c.getNextId(),
			Type:         RPC_JOBTYPE_MAP,
			FileName:     []string{files[index]},
			ReduceNumber: c.ReduceJobNums,
			Version:      0,
		}
		jobMetaInfo := JobMetaInfo{
			JobPtr: &job,
			State:  WAITING,
		}
		if err := c.PutJobMetaInfo(&jobMetaInfo); err != nil {
			return err
		}
		c.MapJobChannel <- &job
	}
	return nil
}

//
// 构造Reduce任务
// 读取本地文件列表，获取 mr-X-Y 格式文件名并存储为 Reduce 任务
//
func (c *Coordinator) CreateReduceJobs() error {
	LOG.Info("CreateReduceJob:: start create ReduceJob")
	for i := 0; i < c.ReduceJobNums; i++ {
		namelist := c.findLocalIntermediateFileList(i)
		job := Job{
			Id:           c.getNextId(),
			Type:         RPC_JOBTYPE_REDUCE,
			FileName:     namelist,
			ReduceNumber: c.ReduceJobNums,
			Version:      0,
		}
		jobMetaInfo := JobMetaInfo{
			JobPtr: &job,
			State:  WAITING,
		}
		if err := c.PutJobMetaInfo(&jobMetaInfo); err != nil {
			return err
		}
		c.ReduceJobChannel <- &job
	}
	return nil
}

//
// 读取 reduceNum 对应的本地文件名列表
//
func (c *Coordinator) findLocalIntermediateFileList(reduceId int) []string {
	dir, _ := os.Getwd()
	files, _ := ioutil.ReadDir(dir)
	res := make([]string, 0)
	if len(files) > 0 {
		for _, file := range files {
			name := file.Name()
			if !file.IsDir() && strings.HasPrefix(name, "mr-") && strings.HasSuffix(name, strconv.Itoa(reduceId)) {
				res = append(res, name)
			}
		}
	}
	return res
}

//
// 获取下一个任务ID
//
func (c *Coordinator) getNextId() int {
	c.JobIndex++
	return c.JobIndex
}

//
// 存放任务元信息到 JobMetaInfoHolder 中
//
func (c *Coordinator) PutJobMetaInfo(info *JobMetaInfo) error {
	id := info.JobPtr.Id
	if c.JobInfoMap.MetaInfoMap[id] != nil {
		LOG.Warn("PutJobMetaInfo:: job metaInfo exists",
			zap.Int("jobId", id))
	} else {
		LOG.Info("PutJobMetaInfo:: put jobMetaInfo successfully",
			zap.Int("jobId", info.JobPtr.Id))
		c.JobInfoMap.MetaInfoMap[id] = info
	}
	return nil
}

//
// 任务状态调整
//
func (j *JobMetaInfoHolder) ChangeJobState(id int, oldStates Condition, newStates Condition) error {
	if j.MetaInfoMap[id] == nil {
		return wrapError(meta.ErrNotFound, "ChangeJobStates:: Not found jobMetaInfo", nil)
	}
	info := j.MetaInfoMap[id]
	if info.State != oldStates {
		LOG.Error(meta.ErrInvalidJobState.String(),
			zap.Int("jobId", id),
			zap.Int("OldState", int(oldStates)),
			zap.Int("CurrentState", int(info.State)))
		return wrapError(meta.ErrInvalidJobState, fmt.Sprintf("ChangeJobState:: invalid job states, oldState=%d, currentState=%d", oldStates, info.State), nil)
	}
	switch newStates {
	case WORKING:
		j.MetaInfoMap[id].StartTime = time.Now()
		j.MetaInfoMap[id].State = WORKING
	case DONE:
		j.MetaInfoMap[id].EndTime = time.Now()
		j.MetaInfoMap[id].State = DONE
	case KILLING:
	}
	return nil
}

//
// Coordinator 状态机调整
//
func (c *Coordinator) ChangeCoordinatorState() bool {
	preState := c.State
	if c.State != ERRORDOWN {
		switch c.State {
		case MAPSTATUS:
			c.State = REDUCESTATUS
			if err := c.CreateReduceJobs(); err != nil {
				LOG.Error("ChangeCoordinatorState:: createReduceJob failed",
					zap.Int("PreState", int(preState)),
					zap.Int("CurrentState", int(c.State)))
				return false
			}
			return true
		case REDUCESTATUS:
			c.State = ALLDONE
			LOG.Info("All job execute success!")
			return true
		}
	}
	return false
}

//
// 容错处理：
//  1. 任务执行过慢（10s超时），超时Coordinator应结束当前任务并重发
//  2. worker出现宕机，任务重发。注意 worker 已经执行完的 Reduce 任务不需要处理、还未处理的 Map任务全部重发
func (c *Coordinator) CrashHandler() {
	for {
		lock.Lock()
		infoMap := c.JobInfoMap.MetaInfoMap
		for index, info := range infoMap {
			if info.State == WORKING && time.Since(info.StartTime) > PERTASK_TIMEOUT*time.Second {
				LOG.Error("CrashHandler:: task execute timeOut",
					zap.Int("jobId", info.JobPtr.Id))
				switch info.JobPtr.Type {
				case RPC_JOBTYPE_MAP:
					c.MapJobChannel <- infoMap[index].JobPtr
					infoMap[index].JobPtr.Version++
					infoMap[index].State = WAITING
				case RPC_JOBTYPE_REDUCE:
					c.ReduceJobChannel <- infoMap[index].JobPtr
					infoMap[index].JobPtr.Version++
					infoMap[index].State = WAITING
				}
			}
		}
		lock.Unlock()
		time.Sleep(1 * time.Second)
	}
}

//
// 检查是否所有 Map/Reduce 任务均结束
//
func (c *Coordinator) checkAllJobDone() bool {
	mapDones := 0
	mapUndos := 0
	reduceDones := 0
	reduceUndos := 0
	for _, job := range c.JobInfoMap.MetaInfoMap {
		switch job.JobPtr.Type {
		case RPC_JOBTYPE_MAP:
			if job.State == DONE {
				mapDones++
			} else if job.State != KILLING {
				mapUndos++
			}
		case RPC_JOBTYPE_REDUCE:
			if job.State == DONE {
				reduceDones++
			} else if job.State != KILLING {
				reduceUndos++
			}
		}
	}
	LOG.Info(fmt.Sprintf("Coordinator:: Map Job has done %d, undo %d\n Reduce Job has done %d, undo %d\n", mapDones, mapUndos, reduceDones, reduceUndos))
	isMapEnd := mapUndos == 0 && mapDones > 0 && reduceDones == 0 && reduceUndos == 0
	isReduceEnd := reduceUndos == 0 && reduceDones > 0 && mapUndos == 0 && mapDones == 0
	isAllEnd := mapUndos == 0 && reduceUndos == 0 && mapDones > 0 && reduceDones > 0
	return isMapEnd || isReduceEnd || isAllEnd
}

//*****************************************RPC Server处理****************************************//
// Worker RPC 远程获取任务
//
func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error {
	lock.Lock()
	defer lock.Unlock()
	switch c.State {
	case MAPSTATUS:
		if len(c.MapJobChannel) > 0 {
			*reply = *<-c.MapJobChannel
			reply.Version++
			c.JobInfoMap.MetaInfoMap[reply.Id].JobPtr.Version++
			if err := c.JobInfoMap.ChangeJobState(reply.Id, WAITING, WORKING); err != nil {
				return err
			}
			LOG.Info("Coordinator RPC:: Success distribute MAPJob",
				zap.Int("JobId", reply.Id))
		} else {
			reply.Type = RPC_JOBTYPE_TRANSER
			reply.Id = -1
			// check whether all mapJob has done
			if c.checkAllJobDone() {
				LOG.Info("Coordinator RPC:: Start change status to ReduceJob, current transfer jobType",
					zap.Int("jobType", reply.Type))
				if !c.ChangeCoordinatorState() {
					LOG.Error("DistributeJob:: change JobState to ReduceJob failed",
						zap.Int("CurrentState", int(c.State)))
				}
			}
		}
	case REDUCESTATUS:
		if len(c.ReduceJobChannel) > 0 {
			*reply = *<-c.ReduceJobChannel
			reply.Version++
			c.JobInfoMap.MetaInfoMap[reply.Id].JobPtr.Version++
			if err := c.JobInfoMap.ChangeJobState(reply.Id, WAITING, WORKING); err != nil {
				return err
			}
			LOG.Info("Coordinator RPC:: Success distribute REDUCEJob",
				zap.Int("JobId", reply.Id))
		} else {
			reply.Type = RPC_JOBTYPE_END
			reply.Id = -1
			if c.checkAllJobDone() {
				LOG.Info("Coordinator RPC:: Start change status to EndJob, current transfer jobType",
					zap.Int("jobType", reply.Type))
				if !c.ChangeCoordinatorState() {
					LOG.Error("DistributeJob:: change JobState to EndJob failed",
						zap.Int("CurrentState", int(c.State)))
				}
			}
		}
	default:
		reply.Type = RPC_JOBTYPE_END
	}
	return nil
}

//
// Worker RPC 反馈任务结束
//
func (c *Coordinator) RemoteJobDone(args *Job, reply *ExampleReply) error {
	LOG.Info("RemoteJob is finishing",
		zap.Int("JobId", args.Id),
		zap.Int("Type", args.Type))
	if args.Version < c.JobInfoMap.MetaInfoMap[args.Id].JobPtr.Version {
		LOG.Warn("Receive Timeout-Job result--------------")
		return nil
	} else {
		lock.Lock()
		lock.Unlock()
		if err := c.JobInfoMap.ChangeJobState(args.Id, WORKING, DONE); err != nil {
			return err
		}
	}
	return nil
}

// 启动 Coordinator线程，作为 RPC Server 端提供服务
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 判断全部任务是否结束
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	lock.Lock()
	defer lock.Unlock()
	return c.State == ALLDONE
}

// 创建 Coordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:            MAPSTATUS,
		MapJobChannel:    make(chan *Job, len(files)),
		ReduceJobChannel: make(chan *Job, nReduce),
		MapJobNums:       len(files),
		ReduceJobNums:    nReduce,
		JobIndex:         0,
		JobInfoMap: &JobMetaInfoHolder{
			make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
		WorkerInfoMap: &WorkerMetaInfoHolder{
			make(map[int]*WorkerMetaInfo, WORKER_NUMS),
		},
	}
	// 构建Map任务
	c.CreateMapJobs(files)
	// 启动RPC Server
	c.server()
	// 启动容错处理
	go c.CrashHandler()
	return &c
}
