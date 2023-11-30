package raft

import "math"

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
	// trans.machine.raft.print("appending %d entries", len(*trans.entries))
	// if prevLogIndex match, but log has different term, remove all log entries in nextIndex and all after that
	trans.machine.removeAfter(int(trans.prevLogIndex) + 1)
	// copy log entries
	trans.machine.appendLogEntry(*trans.entries...)
	// update commitIndex and apply log to stateMachine
	if trans.machine.commitIndex < trans.leaderCommit {
		trans.machine.raft.print("leader commitIndex %d larger than %d", trans.leaderCommit, trans.machine.commitIndex)
		trans.machine.commitIndex = Index(math.Max(float64(trans.leaderCommit), float64(trans.machine.lastLogIndex())))
	}
	trans.machine.tryApplyLog()
	return source
}

func (trans *NewAppendLogEntry) getName() string {
	return "NewLogEntry"
}

func (sm *RaftStateMachine) tryApplyLog() {
	applyLen := sm.commitIndex - sm.lastApplied
	if applyLen > 0 {
		sm.raft.print("apply LogEntry to stateMachine, commitIndex %d applyLen %d", sm.commitIndex, applyLen)
		logBak := make([]Entry, applyLen)
		copy(logBak, sm.log[sm.lastApplied+1:sm.commitIndex+1])
		begin := sm.lastApplied + 1
		go sm.applyEntries(&logBak, int(begin))
	}
}

//
// apply LogEntry to stateMachine, send to applyCh
//
func (sm *RaftStateMachine) applyEntries(entries *[]Entry, startIndex int) {
	for idx, entry := range *entries {
		*sm.applyCh <- ApplyMsg{
			Command:      entry.Command,
			CommandIndex: idx + startIndex,
			CommandValid: true,
		}
	}
	sm.lastApplied = sm.commitIndex
}
