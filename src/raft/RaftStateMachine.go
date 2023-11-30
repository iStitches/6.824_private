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

	// log replication for per peer
	applyCh     *chan ApplyMsg
	log         []Entry // pay attention index 0 is null
	commitIndex Index   // highest index of log entry need to be committed
	lastApplied Index   // highest index of log entry applied to stateMachine

	// volatile state on leaders
	// leader ——> follower
	nextIndex  []Index // index of the next log entry to send to that server，if follower's log is inconsistent with the leader's, reject appendEntriesRPC
	matchIndex []Index // index of highest log entry known to be replicated on server, use for log replication

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

func (rf *Raft) init(applyCh *chan ApplyMsg) {
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
		applyCh:         applyCh,
		log:             make([]Entry, 1),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]Index, rf.PeerCount()),
		matchIndex:      make([]Index, rf.PeerCount()),
	}
	rf.stateMachine.registerStates()
}
