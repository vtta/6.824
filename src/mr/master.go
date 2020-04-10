package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

type WorkerState struct {
	state string
	alive chan bool
}

type Master struct {
	uniqueIdCounter int64
	workers         map[int]*WorkerState
	done            bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// generate a unique id for worker
func (m *Master) uniqueId() int {
	ret := int(m.uniqueIdCounter)
	atomic.AddInt64(&m.uniqueIdCounter, 1)
	return ret
}

// check for worker timeout
func (m *Master) watchDog() {
	for !m.done {
		for k, v := range m.workers {
			select {
			case <-v.alive:
				break
			default:
				delete(m.workers, k)
				log.Print("Deleted timeout worker ", k)
			}
		}
		time.Sleep(time.Second)
	}
}

// Let master to know a worker is alive
// A unique worker id would be assigned on the first heart beat
// Subsequent heart beat require the worker to send id of itself
func (m *Master) Heartbeat(workerId int, reply *int) error {
	if workerId < 0 {
		*reply = m.uniqueId()
		workerId = *reply
		m.workers[workerId] = &WorkerState{"idle", make(chan bool)}
		log.Print("Registered new worker ", workerId)
	} else {
		*reply = 0
	}
	log.Print("Received heart beat from worker ", workerId)
	m.workers[workerId].alive <- true
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{0, map[int]*WorkerState{}, false}

	// Your code here.

	m.server()
	go m.watchDog()
	return &m
}
