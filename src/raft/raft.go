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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = nil
	rf.persist()
}

func (rf *Raft) electionDriver(electionTimeout time.Duration) {
	for ; !rf.killed(); time.Sleep(GUARDTIMEOUT) {
		rf.mu.Lock()
		if time.Now().Sub(rf.timeoutBegin) > electionTimeout {
			switch rf.state {
			case FOLLOWER:
				rf.state = CANDIDATE
				rf.currentTerm += 1
				go rf.leaderElection(rf.currentTerm)
			case CANDIDATE:
				rf.currentTerm += 1
				go rf.leaderElection(rf.currentTerm)
			case LEADER:
			}
			rf.timeoutBegin = time.Now()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) logApplier(applyCh chan ApplyMsg) {
	for ; !rf.killed(); time.Sleep(GUARDTIMEOUT) {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			apply := rf.lastApplied + 1
			RPrintf(rf.currentTerm, rf.me, rf.state, "log applied %v", omittedLogEntry(rf.log, apply))
			applyCh <- ApplyMsg{true, rf.log[apply].Command, apply}
			rf.lastApplied = apply
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) logCommitter(term int) {
	rf.mu.Lock()
	stale := rf.state != LEADER || term != rf.currentTerm
	npeers := len(rf.peers)
	rf.mu.Unlock()

	for ; !rf.killed() && !stale; time.Sleep(GUARDTIMEOUT) {
		rf.mu.Lock()
		stale = rf.state != LEADER || term != rf.currentTerm
		if !stale {
			for n := rf.commitIndex + 1; n <= maxLogIndex(rf.log); n += 1 {
				if e, ok := rf.log[n]; ok && e.Term == rf.currentTerm {
					for i, replicas := 0, 1; i < npeers; i += 1 {
						if i == rf.me {
							continue
						}
						if rf.matchIndex[i] >= n {
							replicas += 1
						}
						if replicas > npeers/2 {
							rf.commitIndex = n
							RPrintf(rf.currentTerm, rf.me, rf.state, "log replicated %v", omittedLogEntry(rf.log, n))
							break
						}
					}
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderElection(term int) {
	rf.mu.Lock()
	rf.votedFor = &rf.me
	RPrintf(rf.currentTerm, rf.me, rf.state, "==== leader election ====")
	rf.persist()

	me := rf.me
	npeers := len(rf.peers)
	lis := logIndexSorted(rf.log)
	lastLogIndex := lis[len(lis)-1]
	lastLogTerm := rf.log[lastLogIndex].Term
	stale := rf.state != CANDIDATE || term != rf.currentTerm
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLogIndex,
		lastLogTerm,
	}
	rf.mu.Unlock()

	votes := make(chan bool, npeers)
	for i := 0; i < npeers; i += 1 {
		if i != me {
			go func(server int) {
				votes <- rf.sendRequestVote(server, &args)
			}(i)
		}
	}

	for voted, agreed := 1, 1; !stale && voted < npeers; voted += 1 {
		yes := <-votes
		rf.mu.Lock()
		stale = rf.state != CANDIDATE || term != rf.currentTerm
		if !stale {
			if yes {
				agreed += 1
			}
			if agreed > npeers/2 {
				rf.state = LEADER
				go rf.logReplication(term)
				stale = true
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) logReplication(term int) {
	rf.mu.Lock()
	RPrintf(rf.currentTerm, rf.me, rf.state, "==== log replication ====")
	maxLogIndex := maxLogIndex(rf.log)
	npeers := len(rf.peers)
	for i := 0; i < npeers; i += 1 {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = maxLogIndex + 1
		rf.matchIndex[i] = 0
	}
	stale := rf.state != LEADER || term != rf.currentTerm
	go rf.logCommitter(term)
	rf.mu.Unlock()

	for ; !rf.killed() && !stale; time.Sleep(HEARTBEATTIMEOUT) {
		rf.mu.Lock()
		stale = rf.state != LEADER || term != rf.currentTerm
		if !stale {
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendAppendEntries(i, rf.newAppendEntriesArg(i))
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) newAppendEntriesArg(server int) *AppendEntriesArgs {
	entries := map[int]LogEntry{}
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	// maybe could support holes in the log?
	for _, idx := range logIndexSorted(rf.log) {
		if idx > prevLogIndex {
			entries[idx] = rf.log[idx]
		}
	}
	return &AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		prevLogIndex,
		prevLogTerm,
		entries,
		rf.commitIndex}
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
	// caller should have been holding the lock while calling this function

	buf := bytes.Buffer{}
	encoder := labgob.NewEncoder(&buf)
	if err := encoder.Encode(rf.currentTerm); err != nil {
		log.Fatalln(err)
	}
	if rf.votedFor != nil {
		if err := encoder.Encode(rf.votedFor); err != nil {
			log.Fatalln(err)
		}
	} else {
		if err := encoder.Encode(math.MinInt32); err != nil {
			log.Fatalln(err)
		}
	}
	if err := encoder.Encode(rf.log); err != nil {
		log.Fatalln(err)
	}
	rf.persister.SaveRaftState(buf.Bytes())

	RPrintf(rf.currentTerm, rf.me, rf.state, "save voted %v votedFor %v log %v", rf.votedFor != nil, rf.votedFor, omittedLog(rf.log))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)

	if err := decoder.Decode(&rf.currentTerm); err != nil {
		log.Fatalln(err)
	}
	if err := decoder.Decode(&rf.votedFor); err != nil {
		log.Fatalln(err)
	}
	if err := decoder.Decode(&rf.log); err != nil {
		log.Fatalln(err)
	}
	if *rf.votedFor < 0 {
		rf.votedFor = nil
	}

	//RPrintf(rf.currentTerm, rf.me, rf.state, "read voted %v votedFor %v log %v", rf.votedFor != nil, rf.votedFor, omittedLog(rf.log))
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//RPrintf(rf.currentTerm,rf.me,rf.state,"recv RequestVote args %v",args)
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		lis := logIndexSorted(rf.log)
		lli := lis[len(lis)-1]
		llt := rf.log[lli].Term
		if args.LastLogTerm > llt || (args.LastLogTerm == llt && args.LastLogIndex >= lli) {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
			rf.timeoutBegin = time.Now()
			rf.persist()
			return
		}
	}
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//RPrintf(rf.currentTerm,rf.me,rf.state,"recv AppendEntries args %v",args)
	// saw higher term
	RPrintf(rf.currentTerm, rf.me, rf.state, "recv AppendEntries args %v", omittedAEA(args))
	defer RPrintf(rf.currentTerm, rf.me, rf.state, "send AppendEntries reply %v", reply)
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply.Term = rf.currentTerm
	// request came from stale leader
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// we are on the same page here, do biz logic
	rf.timeoutBegin = time.Now()
	// check that the entry before received log matches
	if e, ok := rf.log[args.PrevLogIndex]; !ok || e.Term != args.PrevLogTerm {
		reply.Success = false
		//RPrintf(rf.currentTerm,rf.me,rf.state,"prev log entry does match, %v", rf.log)
		// go back a term in the log for a possible matching entry
		// discard empty entry or entries that has the same term as rejected entry
		goBackAllowed := 0
		for term := e.Term; args.PrevLogIndex-goBackAllowed > 0 && (!ok || e.Term == term); {
			goBackAllowed += 1
			e, ok = rf.log[args.PrevLogIndex-goBackAllowed]
		}
		reply.GoBackAllowed = goBackAllowed
		return
	}
	// all entries before received log should match here, append new entries
	indices := logIndexSorted(args.Entries)
	conflict := math.MinInt32
	// delete any conflicts with following entries, enforce that we use the leader's log as THE TRUTH
	for _, idx := range indices {
		if args.Entries[idx].Term != rf.log[idx].Term {
			conflict = idx
			break
		}
	}
	if conflict != math.MinInt32 {
		for _, idx := range logIndexSorted(rf.log) {
			if idx >= conflict {
				delete(rf.log, idx)
			}
		}
	}
	// only put new entries here, bypass duplicate applies
	lastNew := math.MaxInt32
	for _, idx := range indices {
		if _, ok := rf.log[idx]; !ok {
			rf.log[idx] = args.Entries[idx]
			lastNew = idx
		}
	}
	// normal heartbeat RPC should not trigger a persist
	if lastNew != math.MaxInt32 {
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = MinInt(args.LeaderCommit, lastNew)
	}
	//RPrintf(rf.currentTerm,rf.me,rf.state,"log after AppendEntries %v", rf.log)
	reply.Success = true
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
	rf.mu.Lock()
	RPrintf(rf.currentTerm, rf.me, rf.state, "send RequestVote args %v to %v", args, server)
	stale := rf.state != CANDIDATE
	rf.mu.Unlock()
	if stale {
		return false
	}
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok || reply.Term < args.Term {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		RPrintf(rf.currentTerm, rf.me, rf.state, "saw term %v, stepping down", reply.Term)
		rf.stepDown(reply.Term)
		return false
	}
	// reply.Term == args.Term must be true here, since term increase monotonically
	RPrintf(rf.currentTerm, rf.me, rf.state, "recv RequestVote reply %v from %v", reply, server)
	return reply.VoteGranted
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	rf.mu.Lock()
	RPrintf(rf.currentTerm, rf.me, rf.state, "send AppendEntries args %v to %v", omittedAEA(args), server)
	stale := rf.state != LEADER
	rf.mu.Unlock()
	if stale {
		return false
	}
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	// sending failed or stale reply
	if !ok || reply.Term < args.Term {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		RPrintf(rf.currentTerm, rf.me, rf.state, "saw term %v, stepping down", reply.Term)
		rf.stepDown(reply.Term)
		return false
	}
	RPrintf(rf.currentTerm, rf.me, rf.state, "recv AppendEntries reply %v from %v", reply, server)
	// reply.Term == args.Term must be true here, since term increase monotonically
	if reply.Success {
		indices := append([]int{args.PrevLogIndex}, logIndexSorted(args.Entries)...)
		high := indices[len(indices)-1]
		rf.nextIndex[server] = high + 1
		rf.matchIndex[server] = high
	} else {
		rf.nextIndex[server] -= reply.GoBackAllowed
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
		// not fully replicated, retry
		if maxLogIndex(rf.log) >= rf.nextIndex[server] {
			go rf.sendAppendEntries(server, rf.newAppendEntriesArg(server))
		}
	}
	RPrintf(rf.currentTerm, rf.me, rf.state, "server %v nextIndex %v matchIndex %v", server, rf.nextIndex[server], rf.matchIndex[server])
	return reply.Success
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}
	isLeader = true

	// Your code here (2B).
	index = maxLogIndex(rf.log) + 1
	term = rf.currentTerm
	rf.log[index] = LogEntry{term, command}
	rf.persist()
	return
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

	rf.timeoutBegin = time.Now()

	rf.log = map[int]LogEntry{0: {}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	electionTimeout := time.Duration(rand.Int()%500+500) * time.Millisecond
	go rf.electionDriver(electionTimeout)
	go rf.logApplier(applyCh)

	return rf
}
