package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

type WorkerRegisterArgs struct {
}

type WorkerRegisterReply struct {
	WorkerId int
}

type HeartbeatArgs struct {
	WorkerId int
}

type HeartbeatReply struct {
	Shutdown bool
}

type RequestJobArgs struct {
	WorkerId int
}

type RequestJobReply struct {
	HasJob bool
	Job    Job
}

type HandInJobArgs struct {
	WorkerId int
	JobId    int
	Output   []FileSplit
}

type HandInJobReply struct {
}

type MapFn func(string, string) []KeyValue
type ReduceFn func(string, []string) string

type KeyValue struct {
	Key   string
	Value string
}

type Job struct {
	Id int
	// map or reduce
	Kind JobKind
	// partitions of data files
	Data    []FileSplit
	NReduce int
}

type FileSplit struct {
	Name   string
	Offset int64
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

type State int

const (
	UNCHANGED  State = 0
	IDLE       State = 1
	INPROGRESS State = 2
	COMPLETED  State = 3
)

func (s State) String() string {
	switch s {
	case UNCHANGED:
		return "unchanged"
	case IDLE:
		return "idle"
	case INPROGRESS:
		return "in-progress"
	case COMPLETED:
		return "completed"
	default:
		panic(s)
	}
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func counter() (f func() int) {
	i := 0
	return func() int {
		i += 1
		return i
	}
}

// generate a unique id for a worker or a Job
var uniqueId = counter()

func encode(kvs []KeyValue) []byte {
	buf := bytes.Buffer{}
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(&kvs)
	if err != nil {
		log.Fatalln("cannot encode intermediate result", err)
	}
	return buf.Bytes()
}

func decode(b []byte) []KeyValue {
	buf := bytes.Buffer{}
	buf.Write(b)
	decoder := gob.NewDecoder(&buf)
	kvs := []KeyValue{}
	err := decoder.Decode(&kvs)
	if err != nil {
		log.Fatalln("cannot decode intermediate result", err)
	}
	return kvs
}
