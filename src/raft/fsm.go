package raft

import "sync"

//
// this is a stateMachine for leaderElection and requestVote. Peer keeping three states: followerState、startElection、sendHLState(send heartbeat and appendEntries)
//

//
// the stateMachine
//
type StateMachine struct {
	CurState  SMState
	transChan chan SMTransfer // use channel to ensure concurrency safety, using blocked channel to make sure only one state one time
	rwmu      *sync.RWMutex
}

const noTransferState = 900 // stateMachine end

//
// the state change function
//
type SMTransfer interface {
	isRWMu() bool                    // whether need Read/Write Lock
	transfer(source SMState) SMState // transfer state from source
	getName() string                 // return tranfer name
}

//
// the state
//
type SMState int

func (sm *StateMachine) execute() {
	for {
		trans := <-sm.transChan
		// need writeLock
		if trans.isRWMu() {
			sm.rwmu.Lock()
			dest := trans.transfer(sm.CurState)
			if dest != noTransferState {
				sm.CurState = dest
			}
			sm.rwmu.Unlock()
		} else {
			// need readLock
			sm.rwmu.RLock()
			dest := trans.transfer(sm.CurState)
			sm.rwmu.RUnlock()
			if dest != noTransferState {
				sm.rwmu.Lock()
				sm.CurState = dest
				sm.rwmu.Unlock()
			}
		}
	}
}

//
// issue SMTransfer for StateMachine by another goroutine
// if under concurrency, multiple goroutine may issueTrans int the same time, these goroutines need to complete, so use another goroutine
//
func (sm *StateMachine) issueTrans(trans SMTransfer) {
	go func() {
		sm.transChan <- trans
	}()
}
