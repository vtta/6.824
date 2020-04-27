package kvraft

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	leader  int
	kvs     map[string]string
	applied map[int]interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, leader := kv.rf.GetState(); !leader {
		reply.Err = "not a leader"
		return
	}
	if v, ok := kv.kvs[args.Key]; !ok {
		reply.Err = "not found"
	} else {
		reply.Value = v
		reply.Err = ""
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	cmd := Op{args.Key, args.Value, args.Op}
	idx, _, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = "not a leader"
		kv.mu.Unlock()
		return
	}

	okCh := make(chan bool)
	go func() {
		t0 := time.Now()
		for time.Since(t0) < time.Minute && !kv.killed() {
			stale := false
			kv.mu.Lock()
			if v, ok := kv.applied[idx]; ok {
				okCh <- v == cmd
				delete(kv.applied, idx)
				stale = true
			}
			kv.mu.Unlock()
			if stale {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		okCh <- false
	}()
	kv.mu.Unlock()

	if ok := <-okCh; !ok {
		reply.Err = "failed"
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args.Op {
	case "put":
		kv.kvs[args.Key] = args.Value
	case "append":
		kv.kvs[args.Key] = kv.kvs[args.Key] + args.Value
	}
	reply.Err = ""
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.applyCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = map[string]string{}
	kv.applied = map[int]interface{}{}

	go func() {
		for applied := range kv.applyCh {
			if applied.CommandValid {
				kv.applied[applied.CommandIndex] = applied.Command
			}
		}
	}()

	return kv
}
