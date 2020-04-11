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

type WorkerPeer struct {
	state string
	alive chan bool
}

type Master struct {
	uniqueIdCounter int64
	workers         map[int]*WorkerPeer
	done            bool
	jobs            []Job
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
				log.Println("Deleted timeout worker", k)
			}
		}
		time.Sleep(time.Second)
	}
}

// Let master to know a worker is alive
// A unique worker id would be assigned on the first heart beat
// Subsequent heart beat require the worker to send id of itself
func (m *Master) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	*reply = HeartbeatReply{args.WorkerId, false, []Job{}}
	if m.done {
		reply.Shutdown = true
		return nil
	}
	var id int
	if args.WorkerId < 0 {
		// new worker registration
		id = m.uniqueId()
		reply.WorkerId = id
		m.workers[id] = &WorkerPeer{"idle", make(chan bool)}
		log.Println("Registered new worker", id)
	} else {
		// known worker
		id = args.WorkerId
		state := args.StateUpdate
		if state != "" {
			m.workers[id].state = state
		}
	}
	log.Println("Received heart beat from worker", id)
	worker := m.workers[id]
	worker.alive <- true
	if len(m.jobs) > 0 && worker.state != "in-progress" {
		job := m.jobs[0]
		m.jobs = m.jobs[1:]
		reply.Jobs = append(reply.Jobs, job)
		worker.state = "in-progress"
		log.Printf("Assigned %v task on %v to worker %v\n", job.Kind, job.File, id)
	}
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{0, map[int]*WorkerPeer{}, false, []Job{}}
	//nMap := 10 * nReduce

	// Your code here.

	m.server()
	go m.watchDog()

	return &m
}
