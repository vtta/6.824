package raft

import (
	"bytes"
	"fmt"
	"labgob"
	"log"
	"reflect"
	"sort"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func RPrintf(term int, id int, state RaftState, format string, a ...interface{}) {
	if Debug > 0 {
		format = fmt.Sprintf("[%03v] [%v] [%v] ", term, id, state) + format
		log.Printf(format, a...)
	}
}

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

type RaftState int

const (
	FOLLOWER  RaftState = 0
	CANDIDATE RaftState = 1
	LEADER    RaftState = 2
)

func (s RaftState) String() string {
	switch s {
	case FOLLOWER:
		return "F"
	case CANDIDATE:
		return "C"
	case LEADER:
		return "L"
	default:
		panic("unreachable")
	}
}

const (
	GUARDTIMEOUT     = 10 * time.Millisecond
	HEARTBEATTIMEOUT = 200 * time.Millisecond
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      map[int]LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func MinInt(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func logIndexSorted(log map[int]LogEntry) []int {
	ret := []int{}
	for k := range log {
		ret = append(ret, k)
	}
	sort.Ints(ret)
	return ret
}

func omittedLogEntry(log map[int]LogEntry, idx int) struct{ Index, Term int } {
	return struct{ Index, Term int }{idx, log[idx].Term}
}

func omittedLog(log map[int]LogEntry) map[int]int {
	m := map[int]int{}
	for i, e := range log {
		m[i] = e.Term
	}
	return m
}

func omittedAEA(args *AppendEntriesArgs) struct {
	Term,
	LeaderId,
	PrevLogIndex,
	PrevLogTerm int
	Entries      map[int]int
	LeaderCommit int
} {
	return struct {
		Term,
		LeaderId,
		PrevLogIndex,
		PrevLogTerm int
		Entries      map[int]int
		LeaderCommit int
	}{
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, omittedLog(args.Entries), args.LeaderCommit,
	}
}

func encode(val interface{}) []byte {
	buf := bytes.Buffer{}
	encoder := labgob.NewEncoder(&buf)
	err := encoder.Encode(&val)
	if err != nil {
		log.Fatalln(err)
	}
	return buf.Bytes()
}

func decode(b []byte, t reflect.Type) interface{} {
	buf := bytes.NewBuffer(b)
	decoder := labgob.NewDecoder(buf)
	ret := reflect.New(t)
	err := decoder.Decode(&ret)
	if err != nil {
		log.Fatalln("cannot decode intermediate result", err)
	}
	return ret
}
