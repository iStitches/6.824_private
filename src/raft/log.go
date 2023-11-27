package raft

import "6.5840/meta"

//
// 处理过程中缓存的日志数据
//

//
// 日志节点
//
type Entry struct {
	Term    Term
	Command interface{}
}

//
// 日志结构体
//
type Log struct {
	Entries       []Entry
	FirstLogIndex int
	LastLogIndex  int
}

func (l *Log) getLastLogTerm() (Term, error) {
	if l.LastLogIndex >= 0 && l.LastLogIndex >= l.FirstLogIndex && len(l.Entries) > 0 {
		return l.Entries[l.LastLogIndex].Term, nil
	} else {
		return TermNil, nil
	}
	return -1, wrapError(meta.ErrInvalidParam, "invalid logIndex", nil)
}
