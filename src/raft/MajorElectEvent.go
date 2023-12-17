package raft

import (
	"sync"
	"sync/atomic"
)

type SendHLTimeout struct {
	machine *RaftStateMachine
}

func (rf *Raft) makeMajorElected() *SendHLTimeout {
	return &SendHLTimeout{
		machine: rf.stateMachine,
	}
}

func (trans *SendHLTimeout) isRWMu() bool {
	return false
}

//
// init volatile state when peer firstTime electing
// this make sure raft can only submit logEntry in its own term
//
func (machine *RaftStateMachine) initVolatileState(peerCount int) {
	for i := 0; i < peerCount; i++ {
		machine.nextIndex[i] = machine.lastLogIndex() + 1
		machine.matchIndex[i] = 0
	}
}

//
// send heartBeat/appendEntries to all peers parallHL when timeout
//
func (trans *SendHLTimeout) transfer(source SMState) SMState {
	if trans.machine.raft.killed() {
		trans.machine.raft.sendHETimer.Stop()
		return noTransferState
	}
	if source != startSendHEState && source != startElectionState {
		return noTransferState
	}
	// the first time to send HA, init volatileState
	if source == startElectionState {
		trans.machine.raft.print("first sendHL after elect, start init volatileState")
		trans.machine.raft.stateMachine.initVolatileState(trans.machine.raft.PeerCount())
	}
	trans.machine.raft.electionTimer.Stop()
	go trans.machine.raft.sendHLs()
	trans.machine.raft.sendHETimer.Start()
	trans.machine.raft.persist()
	return startSendHEState
}

func (trans *SendHLTimeout) getName() string {
	return "SendHLTimeout"
}

func (rf *Raft) makeSendHLTimeout() *SendHLTimeout {
	return &SendHLTimeout{
		machine: rf.stateMachine,
	}
}

func (t *Timer) setSendHlWait() {
	t.SetWaitMS(RaftHLTimeOut)
}

//
// send HeartBeat/AppendEntries to all peers
//
func (rf *Raft) sendHLs() {
	rf.stateMachine.rwmu.Lock()
	cond := sync.NewCond(&sync.Mutex{})
	joincount := 0
	nextIndexs := make([]Index, len(rf.stateMachine.nextIndex))
	lastSnapshotIndex := rf.stateMachine.lastSnapshotIndex
	prevTerm := rf.stateMachine.currentTerm
	copy(nextIndexs, rf.stateMachine.nextIndex)
	rf.print("leader sendHLs nextIndexs: %v, lastSnapShotIndex: %d", nextIndexs, lastSnapshotIndex)
	rf.stateMachine.rwmu.Unlock()

	for i := 0; i < rf.PeerCount(); i++ {
		if i == rf.me {
			continue
		}
		if nextIndexs[i] <= lastSnapshotIndex {
			// follower is far behind of leader, send InstallSnapshotRpc to replicate snapshot to follower
			rf.print("create one goroutine to send snapshot to %d", i)
			go rf.sendInstallSnapshot(i, nextIndexs[i], prevTerm)
		} else {
			// send log replication request to replicate logEntries to follower
			go rf.sendSingleHL(i, &joincount, cond)
		}
	}
	cond.L.Lock()
	for joincount+1 < rf.PeerCount() {
		cond.Wait()
	}
	cond.L.Unlock()
}

/***********************************sendHLs update******************************************/
// func (rf *Raft) sendHLs() {
// 	rf.stateMachine.rwmu.Lock()
// 	req := AppendEntriesArgs{
// 		Term:         rf.stateMachine.currentTerm,
// 		LeaderId:     rf.me,
// 		Entries:      nil,
// 		LeaderCommit: rf.stateMachine.commitIndex,
// 	}
// 	tag := rf.stateMachine.lastLogIndex()
// 	rf.stateMachine.rwmu.Unlock()
// 	var cnt atomic.Int32
// 	cnt.Store(1)

// 	// send request by parallel
// 	for i, _ := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}
// 		go rf.sendSingleCombine(i, tag, &cnt, &req)
// 	}
// }

//
// combine sendAppendEntries and sendSnapShots, judge whether log can be submitted repeatly
//
func (rf *Raft) sendSingleCombine(server int, lastLogIndex Index, cnt *atomic.Int32, req *AppendEntriesArgs) {
	for {
		rf.stateMachine.rwmu.Lock()
		if rf.stateMachine.CurState != startSendHEState || rf.stateMachine.currentTerm != req.Term {
			rf.stateMachine.rwmu.Unlock()
			break
		}
		nxt := rf.stateMachine.nextIndex[server]

		// follower far behind leader, send InstallSnapshotRPC
		if nxt <= rf.stateMachine.lastSnapshotIndex {
			snapReply := InstallSnapshotReply{}
			snapReq := InstallSnapshotRequest{
				Term:              rf.stateMachine.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.stateMachine.lastSnapshotIndex,
				LastIncludedTerm:  rf.stateMachine.getEntry(rf.stateMachine.lastSnapshotIndex).Term,
				Data:              rf.persister.ReadSnapshot(),
			}
			rf.stateMachine.rwmu.Unlock()
			ok := rf.sendSnapShots(server, &snapReq, &snapReply)
			rf.stateMachine.rwmu.Lock()
			// when send rpc successful, compare replyTerm at first, then update leader's state
			// 1. break if replyTerm > currentTerm, transfer to follower
			// 2. break if replyTerm > AppendEntriesArgs.Term, no need to send AppendEntries again
			// 3. break if nextIndex/term has changed
			// 4. update AppendEntriesArgs before next send
			if ok {
				if snapReply.Term > rf.stateMachine.currentTerm {
					rf.stateMachine.issueTrans(rf.makeLargerTermEvent(rf.stateMachine.currentTerm, server))
					break
				} else if snapReply.Term > req.Term || nxt != rf.stateMachine.nextIndex[server] || rf.stateMachine.currentTerm != req.PrevLogTerm {
					rf.stateMachine.rwmu.Unlock()
					break
				} else {
					rf.stateMachine.nextIndex[server] = snapReq.LastIncludedIndex + 1
					nxt = rf.stateMachine.nextIndex[server]
					req.PrevLogIndex = snapReq.LastIncludedIndex
					req.PrevLogTerm = snapReq.LastIncludedTerm
				}
			} else {
				rf.stateMachine.rwmu.Unlock()
				break
			}
		} else {
			// send appendEntriesRpc normally
			rf.initHLArgsLog(server, req)
		}
		rf.stateMachine.rwmu.Unlock()
		resp := AppendEntriesReply{}
		ok := rf.sendAppendEntires(server, req, &resp)
		if ok {
			rf.stateMachine.rwmu.Lock()
			if resp.Term > rf.stateMachine.currentTerm {
				rf.stateMachine.issueTrans(rf.makeLargerTermEvent(resp.Term, VoteForNil))
			} else {
				rf.stateMachine.issueTrans(rf.makeResolveHLReply(&resp, req, server))
			}
			rf.stateMachine.rwmu.Unlock()
		}
		break
	}
}

//
// send single peer heartBeat/appendEntries Message
//
func (rf *Raft) sendSingleHL(server int, join *int, cond *sync.Cond) {
	sendRpc := false
	rf.stateMachine.rwmu.RLock()
	// if peer's state change, don't send the left appendEntriesRPC
	rf.print("send appendEntry request to %d", server)
	rf.print("lastSnapshotIndex=%d, nextIndex[%d]=%d, send singleHL to %d", rf.stateMachine.lastSnapshotIndex, server, rf.stateMachine.nextIndex[server], server)
	if rf.stateMachine.CurState == startSendHEState && rf.stateMachine.lastSnapshotIndex <= rf.stateMachine.nextIndex[server]-1 {
		sendRpc = true
	}
	rf.stateMachine.rwmu.RUnlock()
	if sendRpc {
		rf.stateMachine.rwmu.RLock()
		args := AppendEntriesArgs{
			Term:     rf.stateMachine.currentTerm,
			LeaderId: rf.me,
		}
		rf.initHLArgsLog(server, &args)
		rf.stateMachine.rwmu.RUnlock()

		// if initHLArgsLog successful, start send rpc
		reply := AppendEntriesReply{}
		rf.print("leader %d send AppendEntriesRPC to %d", rf.me, server)
		ok := rf.sendAppendEntires(server, &args, &reply)
		rf.stateMachine.rwmu.RLock()
		if ok {
			rf.print("receive AppendEntriesRPC reply from %d success %t", server, reply.Success)
			// case curState change
			if reply.Term > rf.stateMachine.currentTerm {
				rf.stateMachine.issueTrans(rf.makeLargerTermEvent(reply.Term, VoteForNil))
			} else {
				// resolve AppendEntriesReply
				rf.stateMachine.issueTrans(rf.makeResolveHLReply(&reply, &args, server))
			}
		} else {
			rf.stateMachine.raft.print("unreachable appendEntries to %d", server)
		}
		rf.stateMachine.rwmu.RUnlock()
	}
	cond.L.Lock()
	*join++
	if *join+1 >= rf.PeerCount() {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

//
// leader init ArgsLog for HL Message
// transfer logEntries that after nextLogIndex
//
func (rf *Raft) initHLArgsLog(server int, args *AppendEntriesArgs) {
	nextIndex := rf.stateMachine.nextIndex[server]
	args.LeaderCommit = rf.stateMachine.commitIndex
	args.PrevLogIndex = nextIndex - 1
	rf.print("server %d prevLogIndex %d leaderCommitIndex %d leaderLastLogIndex %d", server, args.PrevLogIndex, args.LeaderCommit, rf.stateMachine.lastLogIndex())
	args.PrevLogTerm = rf.stateMachine.getEntry(args.PrevLogIndex).Term
	if rf.stateMachine.lastLogIndex() >= nextIndex && rf.stateMachine.lastSnapshotIndex <= nextIndex {
		args.Entries = rf.stateMachine.log[rf.stateMachine.getPhysicalIndex(nextIndex):]
	} else {
		args.Entries = nil
	}
}
