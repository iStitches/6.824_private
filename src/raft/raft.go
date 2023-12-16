package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	stateMachine *RaftStateMachine
	// election timer
	electionTimer *Timer
	// send heartBeat/appendEntries timer
	sendHETimer *Timer
	// print flag
	printFlag     bool
	raftPersister *RaftPersister
}

func (rf *Raft) PeerCount() int {
	return len(rf.peers)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.stateMachine.rwmu.RLock()
	term = int(rf.stateMachine.currentTerm)
	isLeader = rf.stateMachine.CurState == startSendHEState
	rf.stateMachine.rwmu.RUnlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	output := rf.raftPersister.persist(rf.stateMachine)
	rf.persister.SaveRaftState(output)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	// state and logEntries data
	stateOutput := rf.raftPersister.persist(rf.stateMachine)
	rf.persister.Save(stateOutput, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	dataDump := rf.persister.ReadRaftState()
	rf.raftPersister.deserializeState(dataDump, 0, rf.stateMachine)
	rf.raftPersister.deserializeLog(dataDump, stateBinaryOffset, rf.stateMachine)
	rf.print("readPersist %d, currentTerm %d, voteFor %d", rf.me, rf.stateMachine.currentTerm, rf.stateMachine.voteFor)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.stateMachine.rwmu.Lock()
	defer rf.stateMachine.rwmu.Unlock()
	rf.print("start make snapshot, index %d", index)
	if index <= int(rf.stateMachine.lastSnapshotIndex) {
		return
	}
	rf.saveStateAndSnapshot(Index(index), rf.stateMachine.currentTerm, snapshot)
}

//
// RequestVote RPC arguments structure
//
type RequestVoteArgs struct {
	Term         Term
	CandidateId  int
	LastLogIndex Index
	LastLogTerm  Term
}
type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

//
// AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {
	Term         Term
	LeaderId     int
	PrevLogIndex Index // use for log consistency checking
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index
}

type AppendEntriesReply struct {
	Term             Term
	Success          bool
	ConflictLogIndex Index   // when follower lastLogIndex inconsistent with leader, response follower's lastLogIndex
	ConflictLogTerm  Term    // when follower lastLogIndex consistency with leader, but term inconsistent, response follower's lastLogTerm
	Entries          []Entry // the first LogIndex of LogEntry
}

type InstallSnapshotRequest struct {
	Term              Term // leader's Term
	LeaderId          int
	LastIncludedIndex Index // 快照所在的最后日志节点的索引
	LastIncludedTerm  Term
	// Offset            int // 文件块在快照中的偏移量
	Data []byte
	// done bool
}

type InstallSnapshotReply struct {
	Term Term
}

func (rf *Raft) sendSnapShots(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntires(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// InstallSnapshotRPC handler
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.stateMachine.rwmu.Lock()
	reply.Term = rf.stateMachine.currentTerm
	if reply.Term > args.Term {
		return
	}
	// update peer state if its term is smaller than arg.Term
	if args.Term > rf.stateMachine.currentTerm {
		rf.stateMachine.issueTrans(rf.makeLargerTermEvent(args.Term, args.LeaderId))
	}
	rf.print("receive installSnapshotRequest from %d at index %d", args.LeaderId, args.LastIncludedIndex)
	if args.LastIncludedIndex <= rf.stateMachine.lastSnapshotIndex {
		rf.stateMachine.rwmu.Unlock()
		return
	}
	// try to install snapshot from leader, and update commitIndex、lastAppliedIndex
	rf.saveStateAndSnapshot(args.LastIncludedIndex, args.Term, args.Data)
	rf.stateMachine.rwmu.Unlock()
	// apply snapshot to applyChan
	rf.notifyServiceIs(args)
}

//
// AppendEntriesRPC handler
// 1. cope with heartBeat: reject argsTerm < currentTerm;
// 2. cope with logReplication： compare current logIndex&logTerm with PrevLogIndex&PrevLogTerm
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.electionTimer.setElectionWait()
	rf.electionTimer.Start()
	reply.Success = false

	rf.stateMachine.rwmu.RLock()
	defer rf.stateMachine.rwmu.RUnlock()
	reply.Term = rf.stateMachine.currentTerm

	rf.print("receive logEntry from %d prevLogIndex %d prevLogTerm %d term %d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term)

	// for heartBeat, compare argsTerm and currentTerm, pay attention to change peerState at first
	if rf.stateMachine.currentTerm <= args.Term {
		rf.print("encounter larger term %d, transfer to follower", args.Term)
		rf.stateMachine.issueTrans(rf.makeLargerTermEvent(args.Term, args.LeaderId))
		reply.Success = true
	}
	if rf.stateMachine.currentTerm > args.Term {
		rf.print("reply false because leader term %d small than currentTerm %d", args.Term, rf.stateMachine.currentTerm)
		return
	}

	// logCheck：
	// 1. lastLogIndex < PrevLogIndex, find consistent EntryIndex and resend AppendEntriesRPC
	// 2. lastLogIndex == PrevLogIndex, if term is inconsistent, reply inconsistent term and resend AppendEntriesRPC
	// 3. lastLogIndex > PrevLogIndex, check the logEntry in position of PrevLogIndex
	if rf.stateMachine.lastLogIndex() < args.PrevLogIndex {
		rf.print("logIndex smaller than leader %d PrevLogIndex %d PrevLogTerm %d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		reply.ConflictLogIndex = rf.stateMachine.lastLogIndex()
		reply.ConflictLogTerm = rf.stateMachine.getEntry(rf.stateMachine.lastLogIndex()).Term
		reply.Success = false
		return
	}
	// exclude same LogIndex but different LogTerm, find previous logIndex that term isn't equal to prevLogTerm
	if rf.stateMachine.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		rf.print("logTerm inconsistent with leader %d PrevLogIndex %d PrevLogTerm %d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		reply.ConflictLogIndex = rf.stateMachine.searchPreviousTermIndex(args.PrevLogIndex)
		reply.ConflictLogTerm = rf.stateMachine.getEntry(reply.ConflictLogIndex).Term
		reply.Success = false
		return
	}
	// follower start replicate logEntry from leader
	rf.print("follower %d appendLogEntry, ArgsLogLength %d", rf.me, len(args.Entries))
	rf.stateMachine.issueTrans(rf.makeNewAppendLogEntry(int(args.PrevLogIndex), &args.Entries, int(args.LeaderCommit)))
	reply.Success = true
	return
}

//
// RequestVoteRPC handler
// 1. compare requestTerm and currentTerm;
// 2. compare voteFor judge whether to vote;
// 3. fill reply;
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.stateMachine.rwmu.RLock()
	defer rf.stateMachine.rwmu.RUnlock()

	reply.VoteGranted = false
	reply.Term = rf.stateMachine.currentTerm
	stateBool := false
	hasChange := false

	// compare peer's term
	rf.print("%d receive requestVoteRPC from %d, request Term %d", rf.me, args.CandidateId, args.Term)
	// 1. requestTerm > currentTerm, change to followerState
	if rf.stateMachine.currentTerm < args.Term {
		rf.stateMachine.issueTrans(rf.makeLargerTermEvent(args.Term, args.CandidateId))
		stateBool = true
		hasChange = true
	} else if rf.stateMachine.currentTerm > args.Term {
		rf.print("reject vote becauseof lowerTerm: args %d me %d", args.Term, rf.stateMachine.currentTerm)
		stateBool = false
	} else if rf.stateMachine.voteFor == VoteForNil || rf.stateMachine.voteFor == args.CandidateId {
		// compare voteFor
		stateBool = true
	} else {
		stateBool = false
	}
	// compare logTerm and logIndex
	isRequestLogUptoDate := rf.stateMachine.isUptoDate(int(args.LastLogIndex), int(args.LastLogTerm))
	// return result
	if !stateBool {
		rf.print("vote result: reject vote for %d because lower-term", args.CandidateId)
	}
	if !isRequestLogUptoDate {
		rf.print("vote result: reject vote for %d because no-upToDate log", args.CandidateId)
	}
	// compare stateTerm and logTerm&logIndex, finally decide to vote, change peerStatus
	if stateBool && isRequestLogUptoDate {
		reply.VoteGranted = true
		if !hasChange {
			rf.stateMachine.issueTrans(rf.makeLargerTermEvent(args.Term, args.CandidateId))
		}
	}
	return
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// when start, append logEntry into leader peer
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.stateMachine.rwmu.Lock()
	defer rf.stateMachine.rwmu.Unlock()
	isLeader = rf.stateMachine.CurState == startSendHEState
	index = int(rf.stateMachine.lastLogIndex()) + 1
	term = int(rf.stateMachine.currentTerm)
	if isLeader {
		rf.print("receive command %v", command)
		rf.stateMachine.appendLogEntry(Entry{
			Command: command,
			Term:    rf.stateMachine.currentTerm,
		})
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// print raft states：   states | command
//
func (rf *Raft) print(format string, vars ...interface{}) {
	if !rf.printFlag {
		return
	}
	output := fmt.Sprintf(format, vars...)
	// var builder strings.Builder
	// for i := rf.stateMachine.lastSnapshotIndex; i <= rf.stateMachine.commitIndex; i++ {
	// 	entry := rf.stateMachine.log[rf.stateMachine.getPhysicalIndex(i)]
	// 	builder.WriteString(fmt.Sprintf("<%d,%d,%v>, ", i, entry.Term, entry.Command))
	// }
	// fmt.Printf("%d %s term %d voteFor %d lastLogIndex %d lastApplied %d commitIndex %d %v| %s\n", rf.me, rf.stateMachine.stateMachineMap[rf.stateMachine.CurState], rf.stateMachine.currentTerm, rf.stateMachine.voteFor, rf.stateMachine.lastLogIndex(), rf.stateMachine.lastApplied, rf.stateMachine.commitIndex, builder.String(), output)
	fmt.Printf("%d %s term %d voteFor %d lastLogIndex %d lastApplied %d commitIndex %d lastSnapShotIndex %d entries %d | %s\n", rf.me, rf.stateMachine.stateMachineMap[rf.stateMachine.CurState], rf.stateMachine.currentTerm, rf.stateMachine.voteFor, rf.stateMachine.lastLogIndex(), rf.stateMachine.lastApplied, rf.stateMachine.commitIndex, rf.stateMachine.lastSnapshotIndex, rf.stateMachine.logLen(), output)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.printFlag = false

	// one groutine to change randSeed automatically, sleep duration randomlly
	go func() {
		for !rf.killed() {
			rand.Seed(time.Now().UnixNano())
			randMs := 800 + rand.Int()%1000
			time.Sleep(time.Duration(randMs) * time.Millisecond)
		}
	}()

	// init RaftStateMachine、RaftPersister
	rf.init(&applyCh)
	rf.raftPersister = rf.makeRaftPersister()
	// init electionTimer、sendHETimer but not start
	rf.electionTimer = MakeTimer(BaseRaftElectionTimeOut, rf.makeElectionTimeout(), rf)
	rf.sendHETimer = MakeTimer(RaftHLTimeOut, rf.makeSendHLTimeout(), rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// repeat election timeout by another goroutine
	go func() {
		if !rf.killed() {
			randMs := rand.Int() % 500
			time.Sleep(time.Duration(randMs) * time.Millisecond)

			rf.stateMachine.rwmu.RLock()
			rf.stateMachine.raft.print("start election timer")
			rf.stateMachine.rwmu.RUnlock()
			rf.electionTimer.setElectionWait()
			rf.electionTimer.Start()
			go rf.stateMachine.execute()
		}
	}()
	return rf
}
