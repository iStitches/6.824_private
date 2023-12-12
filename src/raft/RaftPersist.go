package raft

import (
	"bytes"

	"6.5840/labgob"
)

// offset of stateBinary
const stateBinaryOffset int = 100

//
// save logEntries、stateMachine into memcache, when
//
type RaftPersister struct {
}

func (rf *Raft) makeRaftPersister() *RaftPersister {
	return &RaftPersister{}
}

//
// save currentTerm、voteFor、LogEntry[] for a machine
//
type RaftStatePersister struct {
	CurrentTerm Term
	VoteFor     int
}

func (rf *RaftPersister) serializeState(stateMachine *RaftStateMachine) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	statePersist := RaftStatePersister{
		CurrentTerm: stateMachine.raft.stateMachine.currentTerm,
		VoteFor:     stateMachine.raft.stateMachine.voteFor,
	}
	if err := e.Encode(statePersist); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rf *RaftPersister) deserializeState(b []byte, offset int, stateMachine *RaftStateMachine) {
	r := bytes.NewBuffer(b[offset:])
	d := labgob.NewDecoder(r)
	obj := RaftStatePersister{}
	if err := d.Decode(&obj); err != nil {
		panic(err)
	}
	stateMachine.currentTerm = obj.CurrentTerm
	stateMachine.voteFor = obj.VoteFor
}

func (rf *RaftPersister) serializeLog(stateMachine *RaftStateMachine) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(stateMachine.log); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rf *RaftPersister) deserializeLog(b []byte, offset int, stateMachine *RaftStateMachine) {
	r := bytes.NewBuffer(b[offset:])
	d := labgob.NewDecoder(r)
	logEntries := make([]Entry, 0)
	if err := d.Decode(&logEntries); err != nil {
		panic(err)
	}
	stateMachine.log = logEntries
}

func (rf *RaftPersister) persist(stateMachine *RaftStateMachine) []byte {
	// serializeLog
	logBinary := rf.serializeLog(stateMachine)
	// serializeState
	stateBinary := rf.serializeState(stateMachine)
	output := make([]byte, stateBinaryOffset+len(logBinary))
	for i, b := range stateBinary {
		output[i] = b
	}
	for i, b := range logBinary {
		output[stateBinaryOffset+i] = b
	}
	return output
}
