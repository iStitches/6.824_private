package raft

type NewAppendLogEntry struct {
	machine      *RaftStateMachine
	entries      *[]Entry
	leaderCommit Index
	prevLogIndex Index
}

func (trans *NewAppendLogEntry) isRWMu() bool {
	return true
}

func (rf *Raft) makeNewAppendLogEntry(prevLogIndex int, entries *[]Entry, leaderCommit int) *NewAppendLogEntry {
	return &NewAppendLogEntry{
		prevLogIndex: Index(prevLogIndex),
		leaderCommit: Index(leaderCommit),
		entries:      entries,
		machine:      rf.stateMachine,
	}
}

func (trans *NewAppendLogEntry) transfer(source SMState) SMState {
	trans.machine.raft.print("appending %d entries", len(*trans.entries))
	// if prevLogIndex match, but log has different term, remove all log entries in nextIndex and all after that
	trans.machine.removeAfter(trans.prevLogIndex + 1)
	// copy log entries
	trans.machine.appendLogEntry(*trans.entries...)
	trans.machine.raft.print("follower appendLogEntry length %d success", len(*trans.entries))
	// update commitIndex and apply log to stateMachine
	if trans.machine.commitIndex < trans.leaderCommit {
		trans.machine.raft.print("leader commitIndex %d larger than me %d", trans.leaderCommit, trans.machine.commitIndex)
		trans.machine.commitIndex = trans.machine.lastLogIndex()
		if trans.machine.commitIndex > trans.leaderCommit {
			trans.machine.commitIndex = trans.leaderCommit
		}
	}
	trans.machine.tryApplyLog()
	trans.machine.raft.persist()
	return noTransferState
}

func (trans *NewAppendLogEntry) getName() string {
	return "NewLogEntry"
}

func (sm *RaftStateMachine) tryApplyLog() {
	applyLen := sm.commitIndex - sm.lastApplied
	if applyLen > 0 {
		sm.raft.print("Apply LogEntry to stateMachine, commitIndex %d applyLen %d", sm.commitIndex, applyLen)
		// deep copy and use another goroutine to applyLog into stateMachine
		logBak := make([]Entry, applyLen)
		copy(logBak, sm.log[sm.getPhysicalIndex(sm.lastApplied+1):sm.getPhysicalIndex(sm.commitIndex+1)])
		begin := sm.lastApplied + 1
		// use another goroutine to append logEntries
		go sm.applyEntries(&logBak, int(begin), int(sm.commitIndex))
	}
}

//
// apply LogEntry to stateMachine, send to applyCh
//
func (sm *RaftStateMachine) applyEntries(entries *[]Entry, startIndex int, commitIndex int) {
	for idx, entry := range *entries {
		*sm.applyCh <- ApplyMsg{
			Command:      entry.Command,
			CommandIndex: idx + startIndex,
			CommandValid: true,
		}
	}
	sm.rwmu.Lock()
	sm.lastApplied = max(sm.lastApplied, Index(commitIndex))
	sm.raft.print("end logEntry apply to stateMachine")
	sm.rwmu.Unlock()
}
