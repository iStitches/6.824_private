package raft

//
// save logEntry
//
type Entry struct {
	Term    Term
	Command interface{}
}

func (sm *RaftStateMachine) getTermByIndex(index int) Term {
	if index >= sm.logLen() || index <= 0 {
		return TermNil
	}
	return sm.log[index].Term
}

func (sm *RaftStateMachine) logLen() int {
	return len(sm.log)
}

func (sm *RaftStateMachine) lastLogIndex() Index {
	return Index(sm.logLen() - 1)
}

func (sm *RaftStateMachine) lastLogTerm() Term {
	return sm.getTermByIndex(int(sm.lastLogIndex()))
}

func (sm *RaftStateMachine) appendLogEntry(entries ...Entry) {
	sm.log = append(sm.log, entries...)
}

// remove log in the position of index and after this
func (sm *RaftStateMachine) removeAfter(index int) {
	if index < sm.logLen() {
		sm.log = sm.log[:index]
	}
}

// search previous logTerm's index
func (sm *RaftStateMachine) searchPreviousTermIndex(index Index) Index {
	curTerm := sm.getTermByIndex(int(index))
	for i := int(index); i > 0; i-- {
		if sm.getTermByIndex(i) != curTerm {
			return Index(i)
		}
	}
	return 0
}

// compare logIndex and logTerm to conclude whether log is uptodate
func (sm *RaftStateMachine) isUptoDate(lastLogIndex int, lastLogTerm int) bool {
	// log with bigger term is more-update
	if sm.lastLogTerm() != Term(lastLogTerm) {
		return sm.lastLogTerm() <= Term(lastLogTerm)
	}
	// log with same-term, which index is bigger and is more-update
	return sm.lastLogIndex() <= Index(lastLogIndex)
}
