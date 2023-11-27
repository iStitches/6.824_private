package mr

import (
	"go.uber.org/zap"
)

const (
	RPC_DISTRIBUTE_JOB = "Coordinator.DistributeJob"
	RPC_FINISH_JOB     = "Coordinator.RemoteJobDone"
)

// 任务类型
const (
	RPC_JOBTYPE_MAP = iota
	RPC_JOBTYPE_REDUCE
	RPC_JOBTYPE_TRANSER
	RPC_JOBTYPE_END
)

// 任务状态
const (
	WAITING = iota
	WORKING
	DONE
	KILLING
)

// Coordinator状态
const (
	MAPSTATUS = iota
	REDUCESTATUS
	ALLDONE
	ERRORDOWN
)

// Worker状态
const (
	ACTIVE = iota
	DOWN
)

// 其它
const (
	PERTASK_TIMEOUT = 5
	WORKER_NUMS     = 3
)

var LOG *zap.Logger
