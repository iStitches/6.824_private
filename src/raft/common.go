package raft

import (
	"time"

	"go.uber.org/zap"
)

type Term int64
type Index int64
type State int

var LOG *zap.Logger

// 初始投票对象
const VoteForNil = -1

// 初始任期
const TermNil = 0

const TimeUnit = time.Millisecond

const (
	// 单次拉票的超时时间
	RaftFetchPacketTimeOut = 100

	// 基础选举随机超时时间
	BaseRaftElectionTimeOut = 800

	// 选举超时时间范围
	RangeRaftElectionTimeOut = 100

	// 心跳/日志 发送间隔时间
	RaftHLTimeOut = 100

	// ticker 间隔时间
	TickerInterval = 100
)
