package mr

import (
	"6.5840/logger"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := &NeedWorkArgs{}

	reply := &NeedWorkReply{}

	for {
		// wait for the coordinator to assign a task
		if err := AskOneTask(args, reply); err != nil {
			logger.Debug(logger.DError, "ask one task failed: %v", err)
		}
		switch reply.Task.Type {
		case MapTask:
			logger.Debug(logger.DInfo, "start map task, map id: %v", reply.Task.TaskId)
			DoMapTask(mapf, reply)
			TaskDone(&FinishWorkArgs{
				Id:       reply.Task.TaskId,
				TaskType: MapTask,
				FileName: fmt.Sprintf("mr-%v-*", reply.Task.TaskId),
			})
			logger.Debug(logger.DInfo, "map task done, map id: %v", reply.Task.TaskId)
		case ReduceTask:
			logger.Debug(logger.DInfo, "start reduce task, reduce id: %v", reply.Task.ReduceId)
			DoReduceTask(reducef, reply)
			TaskDone(&FinishWorkArgs{
				Id:       reply.Task.ReduceId,
				TaskType: ReduceTask,
				FileName: fmt.Sprintf("mr-out-%v", reply.Task.ReduceId),
			})
			logger.Debug(logger.DInfo, "reduce task done, reduce id: %v", reply.Task.ReduceId)
		case Done:
			logger.Debug(logger.DInfo, "receive done task")
			return
		}
	}

}

// read each input file,
// pass it to Map,
// accumulate the intermediate Map output.

func DoMapTask(mapf func(string, string) []KeyValue, reply *NeedWorkReply) {
	filename := reply.Task.FileName
	file, err := os.Open(filename)
	if err != nil {
		logger.Debug(logger.DError, "cannot open %v", filename)
	}
	content, err := os.ReadFile(filename)
	if err != nil {
		logger.Debug(logger.DError, "cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % reply.ReduceCnt
		midFileName := fmt.Sprintf("mr-%v-%v", reply.Task.TaskId, reduceId)
		midFile, _ := os.OpenFile(midFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(midFile)
		enc.Encode(&kv)
		midFile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, reply *NeedWorkReply) {
	dir, _ := os.Getwd()
	tmpfile, err := os.CreateTemp(dir, "mr-out-tmpfile-")
	if err != nil {
		logger.Debug(logger.DError, "create tmpfile failed: %v", err)
	}

	// shuffle
	kva := []KeyValue{}
	for i := 0; i < reply.ReduceCnt; i++ {
		tfName := fmt.Sprintf("mr-%v-%v", i, reply.Task.ReduceId)
		tf, _ := os.Open(tfName)
		dec := json.NewDecoder(tf)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		tf.Close()
	}
	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tmpfile.Close()
	oname := fmt.Sprintf("mr-out-%v", reply.Task.ReduceId)

	if err = os.Rename(tmpfile.Name(), oname); err != nil {
		logger.Debug(logger.DError, "rename tmpfile failed: %v", err)
		os.Remove(tmpfile.Name())
	}
}

func AskOneTask(args *NeedWorkArgs, reply *NeedWorkReply) error {
	ok := call("Coordinator.ResponseOneTask", &args, &reply)

	if ok {
		return nil
	} else {
		return fmt.Errorf("oops! something went wrong!\n")
	}
}

func TaskDone(args *FinishWorkArgs) {
	reply := &FinishWorkReply{}
	call("Coordinator.TaskDone", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
