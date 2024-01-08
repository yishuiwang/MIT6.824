package mr

import (
	"errors"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	count   int
	nReduce int
	files   []string
}

var flag = false

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RpcHandler(args *RpcArgs, reply *RpcReply) error {
	c.count--
	if c.count < 0 {
		//fmt.Println("all file task finished")
		return errors.New("all file task finished")
	}
	//fmt.Println("RpcHandler", c.count)
	reply.FileName = c.files[c.count]
	reply.Seq = c.count
	return nil
}
func (c *Coordinator) RpcDone(args *RpcArgs, reply *RpcReply) error {
	flag = args.Finished
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.

	for !flag {
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//fmt.Println("nReduce is the number of reduce tasks", nReduce)
	c.nReduce = nReduce
	c.files = files
	c.count = len(files)

	c.server()
	return &c
}
