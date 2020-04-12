package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func doMap(fn MapFn, data []FileSplit, nReduce int, mapId int) []FileSplit {
	//log.Printf("begin execution of map Job %v\n", mapId)
	output := []FileSplit{}
	for _, split := range data {
		file, err := os.Open(split.Name)
		if err != nil {
			log.Fatalln("cannot open", split.Name)
		}
		_, err = file.Seek(split.Offset, 0)
		if err != nil {
			log.Fatalln("cannot seek", split.Name)
		}
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
			file, err := ioutil.TempFile(".", "mr-")
			if err != nil {
				log.Fatalln("cannot create temporary file")
			}
			content := encode(kvs)
			//log.Println(filename, content[:8])
			n, err := file.Write(content)
			if err != nil || n != len(content) {
				log.Fatalln("cannot write", file.Name())
			}
			err = file.Close()
			if err != nil {
				log.Fatalln("cannot close", file.Name())
			}
			err = os.Rename(file.Name(), filename)
			if err != nil {
				log.Fatalln("cannot rename to", filename)
			}
			output = append(output, FileSplit{filename, 0})
		}
	}
	//log.Printf("finished execution of map Job %v\n", mapId)
	return output
}

func doReduce(fn ReduceFn, data []FileSplit, reduceId int) []FileSplit {
	//log.Printf("begin execution of reduce Job %v\n", reduceId)
	//log.Println(data, reduceId)
	output := []FileSplit{}
	intermediate := map[string][]string{}
	for _, split := range data {
		file, err := os.Open(split.Name)
		if err != nil {
			log.Fatalln("cannot open", split.Name)
		}
		_, err = file.Seek(split.Offset, 0)
		if err != nil {
			log.Fatalln("cannot seek", split.Name)
		}
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
	file, err := ioutil.TempFile(".", "mr-")
	if err != nil {
		log.Fatalln("cannot create temporary file")
	}
	for k, v := range intermediate {
		r := fn(k, v)
		_, err := fmt.Fprintf(file, "%v %v\n", k, r)
		if err != nil {
			log.Printf("cannot write output '%v %v'\n", k, r)
		}
	}
	err = file.Close()
	if err != nil {
		log.Fatalln("cannot close", filename)
	}
	err = os.Rename(file.Name(), filename)
	if err != nil {
		log.Fatalln("cannot rename to", filename)
	}
	output = append(output, FileSplit{filename, 0})
	//log.Printf("finished execution of reduce Job %v\n", reduceId)
	return output
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
				output := []FileSplit{}
				switch reply.Job.Kind {
				case JK_MAP:
					output = doMap(mapfn, reply.Job.Data, reply.Job.NReduce, reply.Job.Id)
				case JK_REDUCE:
					output = doReduce(reducefn, reply.Job.Data, reply.Job.Id)
				}
				handInReply := HandInJobReply{}
				call("Master.HandInJob", HandInJobArgs{workerId, reply.Job.Id, output}, &handInReply)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	<-shutdown
}
