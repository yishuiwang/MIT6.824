package mr

import (
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
	args := &RpcArgs{
		Request: "hello",
	}

	reply := &RpcReply{}

	// uncomment to send the Example RPC to the coordinator.
	CallCoordinator(args, reply)

	intermediate := []KeyValue{}

	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := os.ReadFile(reply.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.Seq)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	//CallDone()
}

func CallDone() {
	args := &RpcArgs{
		Request:  "job is completely finished",
		Finished: true,
	}

	reply := &RpcReply{}
	ok := call("Coordinator.RpcDone", args, reply)

	if ok {
		//fmt.Println("the worker processes exit.")
	} else {
		//fmt.Printf("oops! something went wrong!\n")
	}
}

func CallCoordinator(args *RpcArgs, reply *RpcReply) {

	ok := call("Coordinator.RpcHandler", &args, &reply)

	if ok {
		//fmt.Println("call success!")
	} else {
		//fmt.Printf("call failed!\n")
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

	fmt.Println(err)
	return false
}
