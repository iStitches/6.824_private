package raft

import "sync"

type SendHLTimeout struct {
	machine *RaftStateMachine
}

func (trans *SendHLTimeout) isRWMu() bool {
	return false
}

//
// init volatile state when peer firstTime electing
//
func (rf *Raft) initVolatileState() {
	for i := 0; i < rf.PeerCount(); i++ {
		rf.stateMachine.nextIndex[i] = rf.stateMachine.lastLogIndex() + 1
		rf.stateMachine.matchIndex[i] = 0
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
		trans.machine.raft.initVolatileState()
	}
	trans.machine.raft.electionTimer.Stop()
	go trans.machine.raft.sendHLs()
	trans.machine.raft.sendHETimer.Start()
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
		if i != rf.me {
			go rf.sendSingleHL(i, &joincount, cond)
		}
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
	if rf.stateMachine.CurState == startSendHEState {
		args := AppendEntriesArgs{
			Term:     rf.stateMachine.currentTerm,
			LeaderId: rf.me,
		}
		rf.initHLArgsLog(server, &args)
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntires(server, &args, &reply)
		if ok {
			rf.print("receive heartBeatRpc reply from %d success %t", server, reply.Success)
			// case curState change
			if reply.Term > rf.stateMachine.currentTerm {
				rf.stateMachine.issueTrans(&LargerTerm{machine: rf.stateMachine, newTerm: reply.Term})
			} else {
				// resolve AppendEntriesReply
				rf.stateMachine.issueTrans(rf.makeResolveHLReply(&reply, &args, server))
			}
		} else {
			rf.stateMachine.raft.print("unreachable appendEntries to %d", server)
		}
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
