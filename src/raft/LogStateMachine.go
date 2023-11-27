package raft

//
// this is logStateMachine for logEntry
//

// type Index uint32

type LogEntry struct {
	Term  Term
	Index Index
}
