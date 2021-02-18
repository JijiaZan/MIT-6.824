package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

// KeyValue a slice of KeyValue retrned by Map function
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) run() {
	for {
		t := w.requestTask()
		if !t.Alive {
			DPrintf("task not alive, exit")
			return
		}
		w.doTask(t)
	}
}

// the worker register with the master, and master assign an ID to the worker
func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if ok := call("Master.RegWorker", &args, &reply); !ok {
		log.Fatal("register failed")
	}
	w.id = reply.WorkerID
}

func (w *worker) requestTask() Task {
	args := TaskArgs{}
	args.WorkerID = w.id
	reply := TaskReply{}
	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		DPrintf("worker get task failed, exit")
		os.Exit(1)
	}
	DPrintf("worker get task: %+v", reply.Task)
	return *reply.Task
}

func (w *worker) doTask(t Task) {
	DPrintf("doing task")
	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase error: +%v", t))
	}
}

func (w *worker) doMapTask(t Task) {
	//read file
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	// do map task
	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	// write the result
	// every mapTask produces nReduce intermediate files
	for idx, reduce := range reduces {
		fileName := reduceName(t.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range reduce {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t Task) {

	// scan the intermediate file called "mr-i-t.seq"
	// collect kvs by key where ihash(key) % nreduce == t.seq
	maps := make(map[string][]string)

	// iterate intermediae "mr-i-t.seq"
	for idx := 0; idx < t.NMap; idx++ {
		fileName := reduceName(idx, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}

		dec := json.NewDecoder(file)

		//read the file  and put the kvs into maps by key
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(outputName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Print("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Phase = t.Phase
	args.WorkerID = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail ")
	}
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
