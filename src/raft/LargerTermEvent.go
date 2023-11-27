package raft

type LargerTerm struct {
	machine   *RaftStateMachine
	newTerm   Term
	newLeader int
}

func (trans *LargerTerm) isRWMu() bool {
	return true
}

//
// transfer from sourceState back to followerState
//
func (trans *LargerTerm) transfer(source SMState) SMState {
	if trans.machine.currentTerm >= trans.newTerm {
		return noTransferState
	}
	trans.machine.currentTerm = trans.newTerm
	trans.machine.voteFor = trans.newLeader
	// stop sendHETimer if it is leader
	trans.machine.raft.sendHETimer.Stop()
	trans.machine.raft.electionTimer.setElectionWait()
	trans.machine.raft.electionTimer.Start()
	return followerState
}

func (trans *LargerTerm) getName() string {
	return "LargerTerm"
}
