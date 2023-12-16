package raft

type ResolveHLReply struct {
	reply   *AppendEntriesReply
	machine *RaftStateMachine
	request *AppendEntriesArgs
	server  int
}

func (trans *ResolveHLReply) isRWMu() bool {
	return true
}

func (rf *Raft) makeResolveHLReply(reply *AppendEntriesReply, request *AppendEntriesArgs, server int) *ResolveHLReply {
	return &ResolveHLReply{
		reply:   reply,
		request: request,
		server:  server,
		machine: rf.stateMachine,
	}
}

func (trans *ResolveHLReply) transfer(source SMState) SMState {
	if trans.reply.Success {
		trans.doSuccess()
	} else {
		trans.doFailure()
	}
	trans.machine.raft.print("nextIndex %v", trans.machine.nextIndex)
	trans.machine.raft.persist()
	return noTransferState
}

func (trans *ResolveHLReply) getName() string {
	return "ResolveHLReply"
}

//
// update each peer's nextIndex、matchIndex when find matchIndex logEntry
// nextIndex = prevLogIndex + 1 + len(entries)
// if over half of peerCounts agree log replication, update commitIndex and apply logEntry to stateMachine
//
func (trans *ResolveHLReply) doSuccess() {
	trans.machine.raft.print("increment %d follower nextIndex by %d", trans.server, len(trans.request.Entries))
	trans.machine.nextIndex[trans.server] = trans.request.PrevLogIndex + Index(1+len(trans.request.Entries))
	trans.machine.matchIndex[trans.server] = trans.machine.nextIndex[trans.server] - 1
	trans.machine.tryCommit()
}

//
// if leader's nextIndex is inconsistent with peer, decrease nextIndex
//
func (trans *ResolveHLReply) doFailure() {
	// fast search match nextIndex
	if trans.reply.ConflictLogIndex < trans.machine.nextIndex[trans.server] {
		trans.machine.nextIndex[trans.server] = trans.reply.ConflictLogIndex + 1
	}
	trans.machine.raft.print("logEntry reject by %d, try again on nextIndex %d next cycle", trans.server, trans.machine.nextIndex[trans.server])
}

//
// compare every peer's matchIndex and leader's commitIndex, if half count agree, update leader's commitIndex
//
func (sm *RaftStateMachine) tryCommit() {
	oldCommitIndex := sm.commitIndex
	newCommitIndex := oldCommitIndex + 1
	if newCommitIndex > sm.lastLogIndex() {
		return
	}
	for {
		agree := 0
		// compare each peerNode matchIndex with leader commitIndex
		for idx, _ := range sm.raft.peers {
			if idx == sm.raft.me {
				continue
			}
			// half of followers agree and leader can submit logEntries in its own term
			if sm.matchIndex[idx] >= newCommitIndex && sm.getEntry(newCommitIndex).Term == sm.currentTerm {
				agree++
			}
		}
		if agree+1 > sm.raft.PeerCount()/2 {
			sm.commitIndex = newCommitIndex
			//sm.raft.print("over half counts peer replicate logEntry success, start apply into stateMachine")
		}
		newCommitIndex++
		if newCommitIndex > sm.lastLogIndex() {
			break
		}
	}
	// judge whether to apply logEntry into stateMachine
	if sm.commitIndex > oldCommitIndex {
		sm.raft.print("update leader commitIndex to %d", sm.commitIndex)
		sm.tryApplyLog()
	}
}
