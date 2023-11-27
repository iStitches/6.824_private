package raft

import "sync"

type SendHLTimeout struct {
	machine *RaftStateMachine
}

func (trans *SendHLTimeout) isRWMu() bool {
	return false
}

//
// send heartBeat/appendEntries to all peers parallHL when timeout
//
func (trans *SendHLTimeout) transfer(source SMState) SMState {
	if source != startSendHEState && source != startElectionState {
		return noTransferState
	}
	// first send heartBeats
	if source == startElectionState {
		trans.machine.raft.print("first sendHL after elect")
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
			go rf.sendSingleHeartBeat(i, &joincount, cond)
		}
	}
	cond.L.Lock()
	for joincount+1 < rf.PeerCount() {
		cond.Wait()
	}
	cond.L.Unlock()
}

//
// send single peer heartBeat Message
//
func (rf *Raft) sendSingleHeartBeat(server int, join *int, cond *sync.Cond) {
	if rf.stateMachine.CurState == startSendHEState {
		args := AppendEntriesArgs{
			Term:     rf.stateMachine.currentTerm,
			LeaderId: rf.me,
		}
		reply := AppendEntriesReply{}
		// rf.print("send heartBeatRpc to %d", server)
		ok := rf.sendAppendEntires(server, &args, &reply)
		if ok {
			rf.print("receive heartBeatRpc reply from %d success %t", server, reply.Success)
			if reply.Term > rf.stateMachine.currentTerm {
				rf.stateMachine.issueTrans(&LargerTerm{machine: rf.stateMachine, newTerm: reply.Term})
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
