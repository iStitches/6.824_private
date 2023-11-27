package raft

import (
	"log"
	"sync"
)

//
// this is an implementation of fsm for RaftStateMachine
//

const (
	followerState      SMState = 901 // followerState is as initial state
	startElectionState SMState = 902 // when startElection
	startSendHEState   SMState = 903 // when start send heartBeat/AppendEntries
)

//
// the stateMachine implementation of Raft
//
type RaftStateMachine struct {
	StateMachine
	// leader election
	raft        *Raft
	currentTerm Term
	voteFor     int

	// log replication
	applyCh     chan ApplyMsg
	log         []Entry
	commitIndex Index // highest index of log entry to be committed
	lastApplied Index // highest index of log entry applied to stateMachine

	// volatile state on leaders
	nextIndex  []Index // index of the next log entry to send to that server
	matchIndex []Index // index of highest log entry known to be replicated on server

	stateMachineMap map[SMState]string
}

func (rf *RaftStateMachine) registerSingleState(state SMState, name string) {
	if _, ok := rf.stateMachineMap[state]; ok {
		log.Fatalf("state %d already in nameMap\n", state)
		return
	}
	rf.stateMachineMap[state] = name
}

func (rf *RaftStateMachine) registerStates() {
	rf.registerSingleState(followerState, "Follower")
	rf.registerSingleState(startElectionState, "Election")
	rf.registerSingleState(startSendHEState, "SendHEEN")
}

// append state to RaftStateMachine
type RaftStateMachineWriter struct {
	machine *RaftStateMachine
}

func (writer *RaftStateMachineWriter) writeState(state SMState) {
	writer.machine.CurState = state
}

// execute RaftStateMachine repeatly
type RaftStateMachineExecutor struct {
	machine *RaftStateMachine
}

func (exeutor *RaftStateMachineExecutor) executeTransfer(source SMState, trans SMTransfer) SMState {
	newState := trans.transfer(source)
	return newState
}

func (rf *Raft) init() {
	rf.stateMachine = &RaftStateMachine{
		StateMachine: StateMachine{
			CurState:  followerState,
			transChan: make(chan SMTransfer),
			rwmu:      new(sync.RWMutex),
		},
		raft:            rf,
		currentTerm:     TermNil,
		voteFor:         VoteForNil,
		stateMachineMap: make(map[SMState]string),
	}
	rf.stateMachine.registerStates()
}
