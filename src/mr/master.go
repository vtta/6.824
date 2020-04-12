package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	jobs map[int]*Job
	mu   sync.Mutex
	// need to be protected by mutex
	mapState map[int]State
	// only keep in-progress worker info
	mapWorker   map[int]int
	mapDone     bool
	reduceState map[int]State
	// only keep in-progress worker info
	reduceWorker map[int]int
	reduceDone   bool
	workerTimer  map[int]*time.Timer
}

func (m *Master) WorkerRegister(args WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker := uniqueId()
	*reply = WorkerRegisterReply{worker}
	m.workerTimer[worker] = time.NewTimer(time.Second)
	log.Printf("[--] [%02v] worker registered\n", worker)

	// clean up guard
	go func(worker int, timer <-chan time.Time) {
		<-timer
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.workerTimer, worker)
		for jobId, workerId := range m.mapWorker {
			if workerId == worker {
				m.mapState[jobId] = IDLE
				delete(m.mapWorker, jobId)
				log.Printf("[%02v] [%02v] job state rolled back\n", jobId, worker)
			}
		}
		for jobId, workerId := range m.reduceWorker {
			if workerId == worker {
				m.reduceState[jobId] = IDLE
				delete(m.reduceWorker, jobId)
				log.Printf("[%02v] [%02v] job state rolled back\n", jobId, worker)
			}
		}
		log.Printf("[--] [%02v] worker disconnected\n", worker)
	}(worker, m.workerTimer[worker].C)
	return nil
}

func (m *Master) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	//log.Printf("[--] [%02v] heartbeat\n", args.WorkerId)
	timer, ok := m.workerTimer[args.WorkerId]
	if !ok {
		return fmt.Errorf("unkown worker")
	}
	timer.Reset(time.Second)
	*reply = HeartbeatReply{m.Done()}
	return nil
}

// ask for a new Job, this call indicates that all previous assigned jobs of this worker have been finished
func (m *Master) RequestJob(args RequestJobArgs, reply *RequestJobReply) error {
	worker := args.WorkerId
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.assignJob(worker)
	if job != nil {
		*reply = RequestJobReply{true, *job}
		log.Printf("[%02v] [%02v] assigned\n", job.Id, worker)
	} else {
		*reply = RequestJobReply{false, Job{}}
	}
	return nil
}

func (m *Master) HandInJob(args HandInJobArgs, reply *HandInJobReply) error {
	jobId := args.JobId
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.jobs[jobId].Kind {
	case JK_MAP:
		_, ok := m.mapWorker[jobId]
		if !ok {
			return fmt.Errorf("illegal job hand-in")
		}
		delete(m.mapWorker, jobId)
		m.mapState[jobId] = COMPLETED
		for _, split := range args.Output {
			var mapId, reduceId int
			n, err := fmt.Sscanf(split.Name, "mr-%d-%d", &mapId, &reduceId)
			if n != 2 || err != nil {
				return fmt.Errorf("ill-formed output files")
			}
			m.jobs[reduceId].Data = append(m.jobs[reduceId].Data, split)
		}
	case JK_REDUCE:
		_, ok := m.reduceWorker[jobId]
		if !ok {
			return fmt.Errorf("illegal job hand-in")
		}
		delete(m.reduceWorker, jobId)
		m.reduceState[jobId] = COMPLETED
	}
	log.Printf("[%02v] [%02v] finished\n", jobId, args.WorkerId)
	return nil
}

// need to have the lock when call this function
func (m *Master) assignJob(worker int) *Job {
	if !m.mapDone {
		mapFinished := true
		for jobId, state := range m.mapState {
			if state == IDLE {
				m.mapState[jobId] = INPROGRESS
				m.mapWorker[jobId] = worker
				return m.jobs[jobId]
			}
			if state != COMPLETED {
				mapFinished = false
			}
		}
		if !mapFinished {
			return nil
		}
		m.mapDone = true
		log.Println("map phase has been completed")
	}
	if !m.reduceDone {
		reduceFinished := true
		for jobId, state := range m.reduceState {
			if state == IDLE {
				m.reduceState[jobId] = INPROGRESS
				m.reduceWorker[jobId] = worker
				return m.jobs[jobId]
			}
			if state != COMPLETED {
				reduceFinished = false
			}
		}
		if reduceFinished {
			m.reduceDone = true
			log.Println("reduce phase has been completed")
		}
	}
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
// if the entire Job has finished.
//
func (m *Master) Done() bool {
	return m.reduceDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		jobs:         map[int]*Job{},
		mu:           sync.Mutex{},
		mapState:     map[int]State{},
		mapWorker:    map[int]int{},
		mapDone:      false,
		reduceState:  map[int]State{},
		reduceWorker: map[int]int{},
		reduceDone:   false,
		workerTimer:  map[int]*time.Timer{},
	}
	for i, file := range files {
		jobId := i + nReduce
		m.jobs[jobId] = &Job{
			jobId, JK_MAP, []FileSplit{{file, 0}}, nReduce,
		}
		m.mapState[jobId] = IDLE
	}
	for i := 0; i < nReduce; i += 1 {
		jobId := i
		m.jobs[jobId] = &Job{
			jobId, JK_REDUCE, []FileSplit{}, nReduce,
		}
		m.reduceState[jobId] = IDLE
	}

	//for jobId, job :=range m.jobs {
	//	log.Println(jobId, job)
	//}

	m.server()

	return &m
}
