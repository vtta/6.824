package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type HeartbeatArgs struct {
	WorkerId int
	State    State
}

type HeartbeatReply struct {
	WorkerId int
	Shutdown bool
	Jobs     []Job
}

type Job struct {
	Id int
	// map or reduce
	Kind JobKind
	// partitions of data files
	Data   []FileSplit
	Worker int
	State  State
}

type JobKind int

const (
	JK_MAP    JobKind = 0
	JK_REDUCE JobKind = 1
)

func (k JobKind) String() string {
	switch k {
	case JK_MAP:
		return "map"
	case JK_REDUCE:
		return "reduce"
	default:
		panic(k)
	}
}

type FileSplit struct {
	Name   string
	Offset int64
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
