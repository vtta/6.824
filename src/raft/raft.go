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
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type RaftState int

const (
	FOLLOWER  RaftState = 0
	CANDIDATE RaftState = 1
	LEADER    RaftState = 2
)

func (s RaftState) String() string {
	switch s {
	case FOLLOWER:
		return "follower"
	case CANDIDATE:
		return "candidate"
	case LEADER:
		return "leader"
	default:
		panic("unreachable")
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         RaftState
	applyCh       chan<- ApplyMsg
	electionTimer *time.Timer
	done          chan bool

	curTerm  int
	votedFor *int
	log      map[int]LogEntry

	commitIndex int
	lastApplied int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.curTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(rf.curTerm); err != nil {
		log.Fatalln("failed while reading persist state")
	}
	if err := d.Decode(rf.votedFor); err != nil {
		log.Fatalln("failed while reading persist state")
	}
	if err := d.Decode(rf.log); err != nil {
		log.Fatalln("failed while reading persist state")
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%200+400) * time.Millisecond
}

func getHeartbeatTimeout() time.Duration {
	return 200 * time.Millisecond
}

//
//func (rf *Raft) follower(electionTimer *time.Timer) {
//	rf.mu.Lock()
//	rf.state = FOLLOWER
//	rf.mu.Unlock()
//
//	if _, ok := <-electionTimer.C; !ok {
//		return
//	}
//	electionTimer.Reset(getElectionTimeout())
//	go rf.candidate(electionTimer)
//}
//
//func (rf *Raft) candidate(electionTimer *time.Timer) {
//	rf.mu.Lock()
//	rf.state = CANDIDATE
//	rf.mu.Unlock()
//
//	leader := make(chan bool)
//	checkStepDown := make(chan bool)
//	go rf.election(leader, checkStepDown)
//	select {
//	case <-electionTimer.C:
//		electionTimer.Reset(getElectionTimeout())
//		go rf.candidate(electionTimer)
//	case <-leader:
//		electionTimer.Stop()
//		go rf.leader()
//	case <-checkStepDown:
//		electionTimer.Reset(getElectionTimeout())
//		go rf.follower(electionTimer)
//	}
//}
//
//func (rf *Raft) leader() {
//	rf.mu.Lock()
//	rf.state = LEADER
//	rf.mu.Unlock()
//
//	for  {
//		rf.replication()
//		time.Sleep(getHeartbeatTimeout())
//	}
//
//}

func (rf *Raft) checkStepDown(term int) {
	if term < rf.curTerm {
		return
	}
	if term == rf.curTerm {
		if rf.state == CANDIDATE {
			rf.votedFor = nil
		}
	} else {
		log.Printf("[%03v] [%v] saw higher term %v\n", rf.curTerm, rf.me, term)
		rf.curTerm = term
		rf.votedFor = nil
		if rf.state == LEADER {
			rf.electionTimer.Reset(getElectionTimeout())
		}
	}
	rf.state = FOLLOWER
}

func (rf *Raft) leader() {
	for isLeader := true; isLeader; _, isLeader = rf.GetState() {
		rf.replication()
		time.Sleep(getHeartbeatTimeout())
	}
}

func (rf *Raft) guard() {
	for {
		select {
		case <-rf.done:
			return
		case <-rf.electionTimer.C:
			go rf.election()
		}
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.curTerm += 1
	rf.votedFor = &rf.me
	rf.electionTimer.Reset(getElectionTimeout())

	args := RequestVoteArgs{rf.curTerm, rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term}
	me := rf.me
	candidateTerm := rf.curTerm
	totalPeers := len(rf.peers)
	rf.mu.Unlock()

	log.Printf("[%03v] [%v] begin election\n", candidateTerm, me)
	votes := make(chan bool, totalPeers)
	lost := make(chan bool, totalPeers)

	for server := 0; server < totalPeers; server += 1 {
		if server == me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				votes <- false
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.curTerm {
				rf.checkStepDown(reply.Term)
				lost <- true
				return
			}
			votes <- reply.VoteGranted
		}(server)
	}

	for yes, voted := 1, 1 ; voted <= totalPeers; {
		select {
		case vote := <-votes:
			voted += 1
			if vote {
				yes += 1
			}
			if yes >= totalPeers/2+1 {
				rf.mu.Lock()
				if rf.state == CANDIDATE && candidateTerm == rf.curTerm {
					rf.state = LEADER
					log.Printf("[%03v] [%v] won the election with %v/%v votes\n", rf.curTerm, rf.me, yes, voted)
					go rf.leader()
				}
				rf.mu.Unlock()
				return
			}
		case <-lost:
			return
		case <-rf.done:
			return
		}
	}
}

func (rf *Raft) replication() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{rf.curTerm, rf.me, 0, nil, 0}
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		//log.Printf("[%03v] [%v] sending heartbeat to %v\n", rf.curTerm, rf.me, peerId)
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(peerId, &args, &reply)
		if reply.Term > rf.curTerm {
			rf.state = FOLLOWER
			rf.curTerm = reply.Term
			return
		}
	}
}

func (rf *Raft) logMoreComplete(peerLastLogIndex, peerLastLogTerm int) bool {
	LastLogIndex, LastLogTerm := rf.commitIndex, rf.log[rf.commitIndex].Term
	return LastLogTerm > peerLastLogTerm ||
		(LastLogTerm == peerLastLogTerm && LastLogIndex > peerLastLogIndex)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%03v] [%v] received RequestVote from %v term %v\n", rf.curTerm, rf.me, args.CandidateId, args.Term)

	rf.checkStepDown(args.Term)

	granted := true
	if args.Term < rf.curTerm || (rf.votedFor != nil && *rf.votedFor != args.CandidateId) {
		granted = false
	}
	if rf.logMoreComplete(args.LastLogIndex, args.LastLogTerm) {
		granted = false
	}

	if granted {
		rf.votedFor = &args.CandidateId
	}
	reply.Term = rf.curTerm
	reply.VoteGranted = granted
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%03v] [%v] received AppendEntries from %v term %v\n", rf.curTerm, rf.me, args.LeaderId, args.Term)
	rf.electionTimer.Reset(getElectionTimeout())

	rf.checkStepDown(args.Term)

	success := true
	if args.Term < rf.curTerm {
		success = false
	}
	entry, ok := rf.log[args.PrevLogIndex]
	if !ok || entry.Term != rf.curTerm {
		success = false
	}
	for idx, entry := range args.Entries {
		if e, ok := rf.log[idx]; !ok || e.Term != entry.Term || !reflect.DeepEqual(e.Command, entry.Command) {
			rf.log[idx] = entry
			for i, _ := range rf.log {
				if i > idx {
					delete(rf.log, i)
				}
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		lastEntryIndex := 0
		for idx := range args.Entries {
			if idx > lastEntryIndex {
				lastEntryIndex = idx
			}
		}
		if args.LeaderCommit < lastEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIndex
		}
	}

	reply.Term = rf.curTerm
	reply.Success = success
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	Entries      map[int]LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	log.Printf("[%03v] [%v] requesting vote from %v\n", rf.curTerm, rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.VoteGranted {
		log.Printf("[%03v] [%v] got vote from %v\n", rf.curTerm, rf.me, server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	once := sync.Once{}
	once.Do(func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		close(rf.done)
		log.Printf("[%03v] [%v] server killed\n", rf.curTerm, rf.me)
	})
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.electionTimer = time.NewTimer(getElectionTimeout())
	rf.log = map[int]LogEntry{0: {}}
	rf.done = make(chan bool)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer.Reset(getElectionTimeout())
	go rf.guard()

	return rf
}
