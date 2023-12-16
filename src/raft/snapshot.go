package raft

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

//
// save state and snapshot
//
func (rf *Raft) saveStateAndSnapshot(index Index, term Term, snapshot []byte) {
	rf.print("saveStateAndSnapshot into stateMachine")
	// adjust logEntries: [lastSnapShotIndex < index < lastLogIndex]
	if index < rf.stateMachine.lastLogIndex() {
		rf.trimLog(Index(index))
	} else {
		// [lastLogIndex < index]
		rf.discardAllLog(index, term, snapshot)
	}

	// update peer state after receive snapshot
	rf.stateMachine.lastSnapshotIndex = Index(index)
	rf.stateMachine.lastApplied = max(rf.stateMachine.lastApplied, Index(index))
	rf.stateMachine.commitIndex = max(rf.stateMachine.commitIndex, Index(index))

	// persist state and snapshot
	rf.persistStateAndSnapshot(snapshot)

}

// //
// // check snapshot data validality
// //
// func (rf *Raft) checkSnapshotValid(index Index, snapshot []byte) bool {
// 	// snapshot Index must be bigger than lastSnapShotIndex
// 	if rf.stateMachine.lastSnapshotIndex > index {
// 		rf.print("snapIndex %d small than lastSnapShotIndex %d", index, rf.stateMachine.lastSnapshotIndex)
// 		return false
// 	}
// 	// snapshotIndex is bigger than lastLogIndex
// 	if rf.stateMachine.lastLogIndex() < index {
// 		return true
// 	}
// 	// // check content
// 	// var d int
// 	// buf := bytes.NewBuffer(snapshot)
// 	// decoder := labgob.NewDecoder(buf)
// 	// if err := decoder.Decode(&d); err != nil {
// 	// 	log.Panicln("decode snapshot failed")
// 	// }
// 	// val := rf.stateMachine.getEntry(Index(index)).Command.(int)
// 	// if d != val {
// 	// 	rf.print("")
// 	// 	log.Panicf("snapshot %d isn't equal to logEntry %d in %d\n", d, index, val)
// 	// }
// 	return true
// }

//
// trim LogEntries before current snapshotIndex
//
func (rf *Raft) trimLog(index Index) {
	rf.print("trimLog and current snapshotIndex %d, lastSnapShotIndex %d", index, rf.stateMachine.lastSnapshotIndex)
	newEntries := make([]Entry, rf.stateMachine.lastLogIndex()-Index(index)+1)
	copy(newEntries, rf.stateMachine.log[rf.stateMachine.getPhysicalIndex(Index(index)):])
	rf.stateMachine.log = newEntries
}

//
// discard logEntries if snapshotIndex is bigger than lastLogIndex
//
func (rf *Raft) discardAllLog(index Index, term Term, snapshot []byte) {
	rf.print("snapshot is more update, discard entire entries")
	rf.stateMachine.log = make([]Entry, 1)
	entry := Entry{
		Term:  term,
		Index: index,
	}
	var val int
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	if err := decoder.Decode(&val); err != nil {
		log.Panicf("decode snapshot failed, err %v\n", err)
	}
	entry.Command = val
	rf.stateMachine.log[0] = entry
}

//
// send InstallSnapShotIndex rpc
//
func (rf *Raft) sendInstallSnapshot(server int, nxt Index, term Term) {
	// construct request and reply structure
	rf.stateMachine.rwmu.Lock()
	rf.print("send snapshotInstall request to %d, nextIndex %d term %d", server, nxt, term)
	snapshot := rf.stateMachine.getEntry(rf.stateMachine.lastSnapshotIndex)
	req := InstallSnapshotRequest{
		Term:              rf.stateMachine.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.stateMachine.lastSnapshotIndex,
		LastIncludedTerm:  snapshot.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.stateMachine.rwmu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendSnapShots(server, &req, &reply)
	if ok {
		rf.stateMachine.rwmu.Lock()
		defer rf.stateMachine.rwmu.Unlock()
		rf.print("receive snapshotRpc reply term %d", reply.Term)
		// leader transfer to follower, elect a new leader
		if reply.Term > rf.stateMachine.currentTerm {
			rf.stateMachine.raft.makeLargerTermEvent(reply.Term, VoteForNil)
			return
		} else if reply.Term > term || nxt != rf.stateMachine.nextIndex[server] || rf.stateMachine.currentTerm != term {
			return
		}
		// adjust follower's nextIndex and matchIndex
		rf.stateMachine.nextIndex[server] = rf.stateMachine.lastSnapshotIndex + 1
		rf.stateMachine.matchIndex[server] = rf.stateMachine.lastSnapshotIndex + 1
	}
}

//
// apply snapshot to stateMachine
//
func (rf *Raft) notifyServiceIs(args *InstallSnapshotRequest) {
	rf.print("notifyServiceIs command")
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: int(args.LastIncludedIndex),
	}
	go func() {
		*rf.stateMachine.applyCh <- msg
	}()
}
