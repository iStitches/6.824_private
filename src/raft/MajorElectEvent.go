package raft

import "sync"

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
	if source != startSendHEState && source != startElectionState {
		return noTransferState
	}
	// the first time to send HA, init volatileState
	if source == startElectionState {
		trans.machine.raft.print("first sendHL after elect, start init volatileState")
		trans.machine.raft.stateMachine.initVolatileState(trans.machine.raft.PeerCount())
	}
	trans.machine.raft.electionTimer.Stop()

	if trans.machine.raft.killed() {
		return noTransferState
	}
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
	cond := sync.NewCond(&sync.Mutex{})
	joincount := 0
	for i := 0; i < rf.PeerCount(); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendSingleHL(i, &joincount, cond)
	}
	cond.L.Lock()
	for joincount+1 < rf.PeerCount() {
		cond.Wait()
	}
	cond.L.Unlock()
}

//
// send single peer heartBeat/appendEntries Message
//
func (rf *Raft) sendSingleHL(server int, join *int, cond *sync.Cond) {
	sendRpc := false
	rf.stateMachine.rwmu.RLock()
	// if peer's state change, don't send the left appendEntriesRPC
	if rf.stateMachine.CurState == startSendHEState {
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
		reply := AppendEntriesReply{}
		rf.print("leader %d send AppendEntriesRPC to %d", rf.me, server)
		ok := rf.sendAppendEntires(server, &args, &reply)
		rf.stateMachine.rwmu.RLock()
		if ok {
			rf.print("receive AppendEntriesRPC reply from %d success %t", server, reply.Success)

			// case curState change
			if reply.Term > rf.stateMachine.currentTerm {
				rf.stateMachine.issueTrans(rf.makeLargerTermEvent(reply.Term, args.LeaderId))
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
	args.PrevLogTerm = rf.stateMachine.getTermByIndex(int(args.PrevLogIndex))
	if rf.stateMachine.lastLogIndex() >= nextIndex {
		args.Entries = rf.stateMachine.log[nextIndex:]
	} else {
		args.Entries = nil
	}
}
