package raft

//
// save logEntry
//
type Entry struct {
	Term    Term
	Command interface{}
	Index   Index
}

func (sm *RaftStateMachine) getPhysicalIndex(index Index) int {
	return int(index) - int(sm.lastSnapshotIndex)
}

func (sm *RaftStateMachine) getEntry(index Index) Entry {
	return sm.log[sm.getPhysicalIndex(index)]
}

func (sm *RaftStateMachine) logLen() int {
	return len(sm.log)
}

func (sm *RaftStateMachine) lastLogIndex() Index {
	return Index(sm.logLen()-1) + sm.lastSnapshotIndex
}

func (sm *RaftStateMachine) appendLogEntry(entries ...Entry) {
	sm.log = append(sm.log, entries...)
}

// remove log in the position of index and after this
func (sm *RaftStateMachine) removeAfter(index Index) {
	phyIndex := sm.getPhysicalIndex(index)
	sm.log = sm.log[:phyIndex]
}

// search previous logTerm's index
func (sm *RaftStateMachine) searchPreviousTermIndex(index Index) Index {
	curTerm := sm.getEntry(index).Term
	for i := index; i > sm.lastSnapshotIndex; i-- {
		if sm.getEntry(i).Term != curTerm {
			return Index(i)
		}
	}
	return 0
}

// func (sm *RaftStateMachine) getConflictLogBlockIndex(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	end := min(args.PrevLogIndex, sm.lastLogIndex())
// 	sqrtLen := int(math.Sqrt(float64(int(end) - int(sm.commitIndex+1))))
// 	reply.Entries = make([]Entry, 0)
// 	for i := int(sm.commitIndex); i <= int(end); i += sqrtLen {
// 		reply.Entries = append(reply.Entries, sm.log[i])
// 	}
// }

// compare logIndex and logTerm to conclude whether log is uptodate
func (sm *RaftStateMachine) isUptoDate(lastLogIndex int, lastLogTerm int) bool {
	// log with bigger term is more-update
	if sm.getEntry(sm.lastLogIndex()).Term != Term(lastLogTerm) {
		return sm.getEntry(sm.lastLogIndex()).Term <= Term(lastLogTerm)
	}
	// log with same-term, which index is bigger and is more-update
	return sm.lastLogIndex() <= Index(lastLogIndex)
}

// func (sm *RaftStateMachine) isUptoDate(lastLogIndex int, lastLogTerm int) bool {
// 	smLastIndex := int(sm.lastLogIndex())
// 	smLastTerm := int(sm.lastLogTerm())
// 	return (smLastTerm < lastLogTerm) || ((smLastTerm == lastLogTerm) && (smLastIndex <= lastLogIndex))
// }
