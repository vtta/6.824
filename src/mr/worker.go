package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

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
