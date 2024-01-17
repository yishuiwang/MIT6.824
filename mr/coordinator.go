package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskTimeout = 10 * time.Second
)

type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	Done       TaskType = "done"
)

type phase int

const (
	duringMap phase = iota
	duringReduce
	finish
)

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	MapTaskCnt    int
	ReduceTaskCnt int
	mu            sync.Mutex
	phase         phase
	//MapTaskChan    chan *Task
	//ReduceTaskChan chan *Task
	task map[TaskType][]*Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) ResponseOneTask(args *NeedWorkArgs, reply *NeedWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceCnt = c.nReduce

	switch c.phase {
	case duringMap:
		t := findAvaliableTask(c.task[MapTask])
		if t != nil {
			reply.Task = *t
		} else {
			reply.Task = Task{Type: string(Done), TaskId: -1}
		}
	case duringReduce:
		t := findAvaliableTask(c.task[ReduceTask])
		if t != nil {
			reply.Task = *t
		} else {
			reply.Task = Task{Type: string(Done), TaskId: -1}
		}
	case finish:
		reply.Task = Task{Type: string(Done), TaskId: -1}
	}
	return nil
}

func findAvaliableTask(tasks []*Task) *Task {
	for _, task := range tasks {
		if task.Status == Waiting || (task.Status == Running && time.Now().UnixNano()-task.StartTime > TaskTimeout.Nanoseconds()) {
			task.StartTime = time.Now().UnixNano()
			task.Status = Running
			return task
		}
	}
	return nil
}
func (c *Coordinator) TaskDone(args *FinishWorkArgs, reply *FinishWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case MapTask:
		id := args.TaskId
		c.task[MapTask][id].Status = Finished
		c.MapTaskCnt--
		//fmt.Println("MapTaskCnt:", c.MapTaskCnt)
		// if all map tasks are finished, start reduce tasks, switch to reduce phase
		if c.MapTaskCnt == 0 {
			c.phase = duringReduce
			for i := 0; i < c.nReduce; i++ {
				t := MakeReduceTask(ReduceTask, i)
				c.task[ReduceTask] = append(c.task[ReduceTask], t)
				//c.ReduceTaskChan <- t
			}
		}
	case ReduceTask:
		id := args.ReduceId
		c.task[ReduceTask][id].Status = Finished
		c.ReduceTaskCnt--
		//fmt.Println("ReduceTaskCnt:", c.ReduceTaskCnt)
		// if all reduce tasks are finished, switch to finish phase
		if c.ReduceTaskCnt == 0 {
			c.phase = finish
		}
	}
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
	ret := false

	// Your code here.
	c.mu.Lock()
	if c.phase == finish {
		fmt.Println("Coordinator Done!")
		ret = true
	}
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskCnt:    len(files),
		ReduceTaskCnt: nReduce,
		nReduce:       nReduce,
		phase:         duringMap,
		//MapTaskChan:    make(chan *Task, len(files)),
		//ReduceTaskChan: make(chan *Task, nReduce),
		task: map[TaskType][]*Task{},
	}

	for i, file := range files {
		t := MakeMapTask(MapTask, file, i)
		c.task[MapTask] = append(c.task[MapTask], t)
		//c.MapTaskChan <- t
	}

	c.server()
	return &c
}

func MakeMapTask(taskType TaskType, fileName string, taskId int) *Task {
	return &Task{
		Type:     string(taskType),
		FileName: fileName,
		TaskId:   taskId,
		Status:   Waiting,
	}
}

func MakeReduceTask(taskType TaskType, reduceId int) *Task {
	return &Task{
		Type:     string(taskType),
		ReduceId: reduceId,
		Status:   Waiting,
	}
}
