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
	state   RaftState
	applyCh chan<- ApplyMsg
	timer   *time.Timer
	done    chan bool

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
	return time.Duration(rand.Int()%500+500) * time.Millisecond
}

func (rf *Raft) beginElection() {
	rf.mu.Lock()
	rf.curTerm += 1
	log.Printf("[%02v] [%v] begin election\n", rf.curTerm, rf.me)
	candidateTerm := rf.curTerm
	rf.votedFor = &rf.me
	rf.timer.Reset(getElectionTimeout())
	rf.state = CANDIDATE
	me := rf.me
	totalPeers := len(rf.peers)
	votesNeed := totalPeers/2 + 1
	votingCh := make(chan bool, totalPeers)
	terminateCh := make(chan bool, totalPeers)
	args := RequestVoteArgs{rf.curTerm, rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term}
	rf.mu.Unlock()

	for peerId := 0; peerId < totalPeers; peerId += 1 {
		if peerId == me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			reply := RequestVoteReply{}
			log.Printf("[%02v] [%v] requesting vote form %v\n", rf.curTerm, rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				votingCh <- false
				return
			}
			if reply.Term > rf.curTerm {
				rf.curTerm = reply.Term
				rf.state = FOLLOWER
				votingCh <- false
				terminateCh <- true
				return
			}
			votingCh <- true
			log.Printf("[%02v] [%v] got vote form %v\n", rf.curTerm, rf.me, server)
		}(peerId)
	}

	yes := 1
	votes := 1
	for {
		select {
		case <-terminateCh:
			return
		case vote := <-votingCh:
			votes += 1
			if vote {
				yes += 1
			}
			if yes >= votesNeed {
				rf.mu.Lock()
				if rf.state == CANDIDATE && candidateTerm == rf.curTerm {
					rf.state = LEADER
					log.Printf("[%02v] [%v] won the election\n", rf.curTerm, rf.me)
					rf.timer.Reset(0)
				}
				rf.mu.Unlock()
				return
			}
			if votes >= totalPeers {
				return
			}
		}
	}
}

func (rf *Raft) beginReplication() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.timer.Reset(100 * time.Millisecond)
	args := AppendEntriesArgs{rf.curTerm, rf.me, 0, nil, 0}
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		//log.Printf("[%02v] [%v] sending heartbeat to %v\n", rf.curTerm, rf.me, peerId)
		rf.sendAppendEntries(peerId, &args, &AppendEntriesReply{})
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	granted := true
	if args.Term < rf.curTerm || (rf.votedFor != nil && *rf.votedFor != args.CandidateId) {
		granted = false
	} else {
		LastLogIndex, LastLogTerm := rf.commitIndex, rf.log[rf.commitIndex].Term
		moreComplete := LastLogTerm > args.LastLogTerm ||
			(LastLogTerm == args.LastLogTerm && LastLogIndex > args.LastLogIndex)
		if moreComplete {
			granted = false
		}
	}

	reply.Term = rf.curTerm
	reply.VoteGranted = granted
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("[%02v] [%v] recevied heartbeat from %v\n", rf.curTerm, rf.me, args.LeaderId)
	rf.curTerm = args.Term
	rf.timer.Reset(getElectionTimeout())
	success := true
	if args.Term < rf.curTerm {
		success = false
	}
	entry, ok := rf.log[args.PrevLogIndex]
	if !ok {
		success = false
	} else if entry.Term != rf.curTerm {
		success = false
		for idx := range rf.log {
			delete(rf.log, idx)
		}
	}
	for idx, entry := range args.Entries {
		if e, ok := rf.log[idx]; !ok || e.Term != entry.Term || !reflect.DeepEqual(e.Command, entry.Command) {
			rf.log[idx] = entry
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.done <- true
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
	rf.timer = time.NewTimer(getElectionTimeout())
	rf.log = map[int]LogEntry{0: {}}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(timer <-chan time.Time, done <-chan bool) {
		for {
			select {
			case <-done:
				return
			case _, ok := <-timer:
				if !ok {
					return
				}
				if _, isLeader := rf.GetState(); isLeader {
					go rf.beginReplication()
				} else {
					go rf.beginElection()
				}
			}

		}
	}(rf.timer.C, rf.done)
	return rf
}
