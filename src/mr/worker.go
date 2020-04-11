package mr

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

func doMap(fn MapFn, data []FileSplit, nReduce int, mapId int) {
	log.Printf("Begining execution of map job %v\n", mapId)
	for _, split := range data {
		file, err := os.Open(split.Name)
		if err != nil {
			log.Fatalln("cannot open", split.Name)
		}
		file.Seek(split.Offset, 0)
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalln("cannot read", split.Name)
		}
		kvs := fn(split.Name, string(content))
		intermediate := map[int][]KeyValue{}
		for _, kv := range kvs {
			intermediate[ihash(kv.Key) % nReduce] = append(intermediate[ihash(kv.Key) % nReduce], kv)
		}
		for reduceId, kvs := range intermediate {
			// "mr-X-Y": X is the Map jobId, and Y is the reduce jobId
			filename := fmt.Sprintf("mr-%v-%v", mapId, reduceId)
			file, err := os.Create(filename)
			if err != nil {
				log.Fatalln("cannot create", filename)
			}
			content := encode(kvs)
			//log.Println(filename, content[:8])
			n, err := file.Write(content)
			if err != nil || n != len(content) {
				log.Fatalln("cannot write", filename)
			}
			err = file.Close()
			if err != nil {
				log.Fatalln("cannot close", filename)
			}
		}
	}
	log.Printf("Finished execution of map job %v\n", mapId)

}

func doReduce(fn ReduceFn, data []FileSplit, reduceId int) {
	log.Printf("Begining execution of reduce job %v\n", reduceId)
	//log.Println(data, reduceId)
	intermediate := map[string][]string{}
	for _, split := range data {
		file, err := os.Open(split.Name)
		if err != nil {
			log.Fatalln("cannot open", split.Name)
		}
		file.Seek(split.Offset, 0)
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalln("cannot read", split.Name)
		}
		//log.Println(split.Name, content[:8])
		kvs := decode(content)
		for _, kv := range kvs {
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
	}
	filename := fmt.Sprintf("mr-out-%v", reduceId)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalln("cannot create", filename)
	}
	for k, v := range intermediate {
		fmt.Fprintf(file, "%v %v\n", k, fn(k, v))
	}
	err = file.Close()
	if err != nil {
		log.Fatalln("cannot close", filename)
	}
	log.Printf("Finished execution of reduce job %v\n", reduceId)

}

func encode(kvs []KeyValue) []byte {
	buf := bytes.Buffer{}
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(&kvs)
	if err != nil {
		log.Fatalln("cannot encode intermediate result",err)
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
		log.Fatalln("cannot decode intermediate result",err)
	}
	return kvs
}

//
// main/mrworker.go calls this function.
//
func Worker(mapfn MapFn, reducefn ReduceFn) {
	jobs := make(chan Job)
	done := make(chan bool)
	shutdown := make(chan bool)
	handleAssignedJob := func(reply HeartbeatReply) {
		// log.Println("Got reply", reply)
		for _, v := range reply.Jobs {
			//log.Printf("Received %v job %v on %v\n", v.Kind, v.Id, v.Data)
			log.Printf("Received %v job %v\n", v.Kind, v.Id)
		}
		go func(reply HeartbeatReply) {
			for _, v := range reply.Jobs {
				jobs <- v
			}
		}(reply)
	}
	reply := HeartbeatReply{}
	call("Master.Heartbeat", HeartbeatArgs{-1, IDLE}, &reply)
	workerId := reply.WorkerId
	log.Println("Registered with id", workerId)
	handleAssignedJob(reply)
	// heartbeat thread
	go func() {
		args := HeartbeatArgs{}
		reply := HeartbeatReply{}
		for {
			time.Sleep(50 * time.Millisecond)
			select {
			case <-done:
				args = HeartbeatArgs{workerId, COMPLETED}
			default:
				args = HeartbeatArgs{workerId, UNCHANGED}
			}
			call("Master.Heartbeat", args, &reply)
			handleAssignedJob(reply)
			if reply.Shutdown {
				shutdown <- true
				return
			}
		}
	}()
	// pulling for job in a blocking style
	go func() {
		for {
			job := <-jobs
			switch job.Kind {
			case JK_MAP:
				doMap(mapfn, job.Data, job.NReduce, job.Id)
			case JK_REDUCE:
				doReduce(reducefn, job.Data, job.Id)
			}
			done <- true
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
