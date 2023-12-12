package raft

import (
	"math/rand"
	"sync"
)

type ElectionTimeOut struct {
	machine *RaftStateMachine
}

func (rf *Raft) makeElectionTimeout() *ElectionTimeOut {
	return &ElectionTimeOut{
		machine: rf.stateMachine,
	}
}

//
// reset random electionTimeOut
//
func (t *Timer) setElectionWait() {
	t.SetWaitMS(BaseRaftElectionTimeOut + rand.Int()%RangeRaftElectionTimeOut)
}

//
// transfer from follower to candidate
//
func (trans *ElectionTimeOut) transfer(source SMState) SMState {
	// initialState must be followerState or startElectionState
	if source != followerState && source != startElectionState {
		trans.machine.raft.print("not transfer from follower or startElection")
		return noTransferState
	}
	trans.machine.raft.print("begin election")
	trans.machine.raft.electionTimer.Stop()

	// change raft state data
	trans.machine.currentTerm++
	trans.machine.voteFor = trans.machine.raft.me

	if trans.machine.raft.killed() {
		return noTransferState
	}
	// reset electionTimer and restart
	trans.machine.raft.electionTimer.setElectionWait()
	//trans.machine.raft.print("waitMs: %d", trans.machine.raft.electionTimer.waitMS)
	trans.machine.raft.electionTimer.Start()

	// send requestVoteRPC to other server by parallel, use another goroutine to escape from blocking main electTimeOut goroutine
	go trans.machine.raft.doElect()
	trans.machine.raft.persist()
	return startElectionState
}

func (trans *ElectionTimeOut) isRWMu() bool {
	return true
}

func (trans *ElectionTimeOut) getName() string {
	return "ElectionTImeOut"
}

//
// send requestVote request to each peers by parallel
//
func (rf *Raft) sendRequestVotePerOne(votes *int, join *int, elected *bool, server int, cond *sync.Cond) {
	rf.stateMachine.rwmu.RLock()
	// build dataStructure
	req := RequestVoteArgs{
		Term:         rf.stateMachine.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.stateMachine.lastLogIndex(),
		LastLogTerm:  rf.stateMachine.lastLogTerm(),
	}
	reply := RequestVoteReply{}
	rf.stateMachine.raft.print("send requestVote to %d", server)
	rf.stateMachine.rwmu.RUnlock()

	// Error！: if elected, skip send requestVoteRPC, must send rpc despite of leader electing
	// cond.L.Lock()
	// if !*elected {
	// 	rpcSend = true
	// }
	// cond.L.Unlock()

	ok := rf.sendRequestVote(server, &req, &reply)
	rf.stateMachine.rwmu.RLock()
	if ok {
		// transfer from candidate to follower if currentTerm is lower
		if reply.Term > rf.stateMachine.currentTerm {
			rf.stateMachine.issueTrans(rf.makeLargerTermEvent(reply.Term, server))
		} else {
			rf.print("voteReply: %d replyto %d ok, grant %t", server, rf.me, reply.VoteGranted)
			if reply.VoteGranted {
				cond.L.Lock()
				*votes++
				cond.L.Unlock()
			}
			// if votes receive from majority peers, become leader
			cond.L.Lock()
			if *votes+1 > rf.PeerCount()/2 {
				if !*elected {
					rf.print("become leader: %d", rf.me)
					rf.stateMachine.issueTrans(rf.makeMajorElected())
					*elected = true
				}
			}
			cond.L.Unlock()
		}
	} else {
		rf.stateMachine.raft.print("unreachable requestVote to %d", server)
	}
	rf.stateMachine.rwmu.RUnlock()

	cond.L.Lock()
	*join++
	if *join+1 >= len(rf.peers) {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

//
// send request vote to other peer
// use cond to ensure requestVoteRPC send to all peers
//
func (rf *Raft) doElect() {
	voteCount := 0
	joinCount := 0
	elected := false
	// use cond to ensure sending len(rf.peers) counts requestVoteRequest
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < rf.PeerCount(); i++ {
		if i == rf.me {
			continue
		}
		// send requestVoteRpc by another goroutine
		go rf.sendRequestVotePerOne(&voteCount, &joinCount, &elected, i, cond)
	}
	// before wait, we need lock first
	cond.L.Lock()
	// use condition to ensure all peers has send requestVoteRPC
	for joinCount+1 < len(rf.peers) {
		cond.Wait()
	}
	cond.L.Unlock()
}
