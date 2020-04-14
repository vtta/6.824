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
	"labrpc"
	"math/rand"
	"sort"
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
	Command []byte
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

const (
	GUARDTIMEOUT     = 10 * time.Millisecond
	HEARTBEATTIMEOUT = 200 * time.Millisecond
)

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

	state        RaftState
	timeoutBegin time.Time

	currentTerm int
	votedFor    *int
	log         map[int]LogEntry
}

func (rf *Raft) logIndexSorted() []int {
	keys := []int{}
	for k, _ := range rf.log {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = &rf.me

	electionTerm := rf.currentTerm
	me := rf.me
	npeers := len(rf.peers)
	lis := rf.logIndexSorted()
	lastLogIndex := lis[len(lis)-1]
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLogIndex,
		lastLogTerm,
	}
	RPrintf(rf.currentTerm, rf.me, "leader election")
	rf.mu.Unlock()

	votes := make(chan bool, npeers)
	for i := 0; i < npeers; i += 1 {
		if i != me {
			go func(server int) {
				votes <- rf.sendRequestVote(server, &args)
			}(i)
		}
	}

	for voted, agreed := 1, 1; voted < npeers; voted += 1 {
		yes := <-votes
		if yes {
			agreed += 1
		}
		if agreed > npeers/2 {
			rf.mu.Lock()
			if electionTerm == rf.currentTerm {
				go rf.logReplication()
			}
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) logReplication() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = LEADER
	rf.votedFor = nil
	RPrintf(rf.currentTerm, rf.me, "log replication")

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		lis := rf.logIndexSorted()
		lli := lis[len(lis)-1]
		llt := rf.log[lli].Term
		if llt > args.LastLogTerm || (llt == args.LastLogTerm && lli >= args.LastLogIndex) {
			reply.VoteGranted = true
			return
		}
	}
	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) bool {
	RPrintf(rf.currentTerm, rf.me, "requesting vote from %v\n", server)
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok || reply.Term < args.Term {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		RPrintf(rf.currentTerm, rf.me, "saw term %v, stepping down\n", reply.Term)
		rf.stepDown(args.Term)
		rf.votedFor = nil
		return false
	}
	RPrintf(rf.currentTerm, rf.me, "vote reply %v from %v\n", reply, server)
	return reply.Term == args.Term && reply.VoteGranted
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.timeoutBegin = time.Now()
	rf.log = map[int]LogEntry{0: {}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	electionTimeout := time.Duration(rand.Int()%200+400) * time.Millisecond
	go func() {
		for !rf.killed() {
			time.Sleep(GUARDTIMEOUT)
			rf.mu.Lock()
			if time.Now().Sub(rf.timeoutBegin) > electionTimeout {
				switch rf.state {
				case FOLLOWER:
					if rf.votedFor == nil {
						go rf.leaderElection()
					}
				case CANDIDATE:
					go rf.leaderElection()
				case LEADER:
				}
				rf.timeoutBegin = time.Now()
			}
			rf.mu.Unlock()
		}
	}()

	return rf
}
