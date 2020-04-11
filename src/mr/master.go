package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

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

type WorkerPeer struct {
	id    int
	state State
	alive chan bool
	job   *Job
}

type Master struct {
	mu sync.Mutex
	uniqueIdCounter int64
	workers         map[int]*WorkerPeer
	jobs            map[int]*Job
	mapJobIds       []int
	reduceJobIds    []int
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
	return m.done
}

// generate a unique id for a worker or a job
func (m *Master) uniqueId() int {
	ret := int(m.uniqueIdCounter)
	atomic.AddInt64(&m.uniqueIdCounter, 1)
	return ret
}

// check for worker timeout
func (m *Master) watchDog() {
	for !m.done {
		m.mu.Lock()
		for k, v := range m.workers {
			select {
			case <-v.alive:
				break
			default:
				if m.jobs[v.job.Id].State == INPROGRESS {
					m.jobs[v.job.Id].State = IDLE
					log.Printf("Reset %v job %v of timeout worker %v\n", v.job.Kind, v.job.Id, k)
				}
				delete(m.workers, k)
				log.Println("Deleted timeout worker", k)
			}
		}
		m.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
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
	m.mu.Lock()
	var workerId int
	if args.WorkerId < 0 {
		// new worker registration
		workerId = m.uniqueId()
		reply.WorkerId = workerId
		m.workers[workerId] = &WorkerPeer{workerId, IDLE, make(chan bool), nil}
		log.Println("Registered new worker", workerId)
	} else {
		// known worker
		workerId = args.WorkerId
		newState := args.State
		if newState != UNCHANGED {
			m.workers[workerId].state = newState
		}
		if newState == COMPLETED {
			jobId := m.workers[workerId].job.Id
			m.jobs[jobId].State = COMPLETED
			log.Printf("Job %v has completed by worker %v\n", jobId, workerId)
		}
	}
	//log.Println("Received heartbeat from worker", workerId)
	worker := m.workers[workerId]
	if worker.state != INPROGRESS {
		job := m.pendingJob()
		if job != nil {
			job.State = INPROGRESS
			job.Worker = workerId
			worker.job = job
			reply.Jobs = append(reply.Jobs, *worker.job)
			worker.state = INPROGRESS
			log.Printf("Assigned %v task %v on %v to worker %v\n", worker.job.Kind, worker.job.Id, worker.job.Data, workerId)
		}
	}
	m.mu.Unlock()
	worker.alive <- true
	// log.Println("Sent reply", reply)
	return nil
}

// find a available job to assign
// treat map and reduce as two non-overlapping phase
// only assign reduce jobs after map phase finished
func (m *Master) pendingJob() *Job {
	mapFinished := true
	for _, v := range m.mapJobIds {
		state := m.jobs[v].State
		if state == IDLE {
			return m.jobs[v]
		}
		if state != COMPLETED {
			mapFinished = false
		}
	}
	if !mapFinished {
		return nil
	}
	log.Println("Map phase has been completed")
	reduceFinished := true
	for _, v := range m.reduceJobIds {
		state := m.jobs[v].State
		if state == IDLE {
			return m.jobs[v]
		}
		if state != COMPLETED {
			reduceFinished = false
		}
	}
	if reduceFinished {
		m.done = true
	}
	log.Println("Reduce phase has been completed")
	//log.Println("Whole MapReduce program has been completed")
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{sync.Mutex{},int64(nReduce), map[int]*WorkerPeer{}, map[int]*Job{}, []int{}, []int{}, false}
	for _, file := range files {
		jobId := m.uniqueId()
		m.mapJobIds = append(m.mapJobIds, jobId)
		m.jobs[jobId] = &Job{
			jobId, JK_MAP, []FileSplit{{file, 0}}, -1, IDLE, nReduce,
		}
	}
	for i := 0; i < nReduce; i += 1 {
		jobId := i
		m.reduceJobIds = append(m.reduceJobIds, jobId)
		data := []FileSplit{}
		for _, j := range m.mapJobIds {
			// naming convention for intermediate files: "mr-X-Y"
			// where X is the Map jobId, and Y is the reduce jobId
			data = append(data, FileSplit{fmt.Sprintf("mr-%v-%v", j, i), 0})
		}
		m.jobs[jobId] = &Job{
			jobId, JK_REDUCE, data, -1, IDLE, nReduce,
		}
	}
	//for k, v := range m.jobs {
	//	log.Println(k, v)
	//}
	//log.Println("mapJobIds", m.mapJobIds)
	//log.Println("reduceJobIds", m.reduceJobIds)

	m.server()
	go m.watchDog()

	return &m
}
