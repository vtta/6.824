package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func doMap(fn MapFn, data []FileSplit, nReduce int, mapId int) {
	log.Printf("Begining execution of map Job %v\n", mapId)
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
			intermediate[ihash(kv.Key)%nReduce] = append(intermediate[ihash(kv.Key)%nReduce], kv)
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
	log.Printf("Finished execution of map Job %v\n", mapId)

}

func doReduce(fn ReduceFn, data []FileSplit, reduceId int) {
	log.Printf("Begining execution of reduce Job %v\n", reduceId)
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
	log.Printf("Finished execution of reduce Job %v\n", reduceId)

}

func callRegister() int {
	reply := WorkerRegisterReply{}
	call("Master.WorkerRegister", WorkerRegisterArgs{}, &reply)
	return reply.WorkerId
}

//
// main/mrworker.go calls this function.
//
func Worker(mapfn MapFn, reducefn ReduceFn) {
	workerId := callRegister()
	shutdown := make(chan bool)
	go func(shutdown chan<- bool) {
		args := HeartbeatArgs{workerId}
		reply := HeartbeatReply{}
		for {
			ok := call("Master.Heartbeat", args, &reply)
			if !ok || reply.Shutdown {
				shutdown <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}(shutdown)
	go func() {
		for {
			args := RequestJobArgs{workerId}
			reply := RequestJobReply{}
			call("Master.RequestJob", args, &reply)
			if reply.HasJob {
				switch reply.Job.Kind {
				case JK_MAP:
					doMap(mapfn, reply.Job.Data, reply.Job.NReduce, reply.Job.Id)
				case JK_REDUCE:
					doReduce(reducefn, reply.Job.Data, reply.Job.Id)
				}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	<-shutdown
}
