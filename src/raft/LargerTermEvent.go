package raft

type LargerTerm struct {
	machine   *RaftStateMachine
	newTerm   Term
	newLeader int
}

func (trans *LargerTerm) isRWMu() bool {
	return true
}

func (rf *Raft) makeLargerTermEvent(newTerm Term, newLeader int) *LargerTerm {
	return &LargerTerm{
		machine:   rf.stateMachine,
		newTerm:   newTerm,
		newLeader: newLeader,
	}
}

//
// transfer from sourceState back to followerState
//
func (trans *LargerTerm) transfer(source SMState) SMState {
	//pay attention, when need to deal with LargerTermEvent, the currentTerm may change, and only bigger change need to reset state、timer
	if trans.machine.currentTerm > trans.newTerm {
		return noTransferState
	}
	trans.machine.raft.print("%d transfer to followerState", trans.machine.raft.me)
	trans.machine.currentTerm = trans.newTerm
	trans.machine.voteFor = trans.newLeader
	// stop sendHETimer if it is leader
	trans.machine.raft.sendHETimer.Stop()
	trans.machine.raft.electionTimer.setElectionWait()
	trans.machine.raft.electionTimer.Start()
	trans.machine.raft.persist()
	return followerState
}

func (trans *LargerTerm) getName() string {
	return "LargerTerm"
}
