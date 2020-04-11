package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

type MapFn func(string, string) []KeyValue
type ReduceFn func(string, []string) string

func doMap(fn MapFn, data []FileSplit) {

}

func doReduce(fn ReduceFn, data []FileSplit) {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapfn MapFn, reducefn ReduceFn) {
	args := HeartbeatArgs{-1, IDLE}
	reply := HeartbeatReply{}
	call("Master.Heartbeat", args, &reply)
	workerId := reply.WorkerId
	jobs := make(chan Job)
	done := make(chan bool)
	shutdown := make(chan bool)
	// heartbeat thread
	go func() {
		args := HeartbeatArgs{}
		reply := HeartbeatReply{}
		for {
			select {
			case <-done:
				args = HeartbeatArgs{workerId, COMPLETED}
			default:
				args = HeartbeatArgs{workerId, UNCHANGED}
			}
			call("Master.Heartbeat", args, &reply)
			go func() {
				for _, v := range reply.Jobs {
					jobs <- v
				}
			}()
			if reply.Shutdown {
				shutdown <- true
			}
		}
	}()
	// pulling for job, if none wait for a heartbeat and try again
	go func() {
		for {
			select {
			case job := <-jobs:
				switch job.Kind {
				case "map":
					doMap(mapfn, job.Data)
				case "reduce":
					doReduce(reducefn, job.Data)
				}
				done <- true
			default:
				time.Sleep(time.Second)
			}
		}
	}()
	<-shutdown
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
