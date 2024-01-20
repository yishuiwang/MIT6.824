package mr

import (
	"6.5840/logger"
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskTimeout = 10 * time.Second
)

type phase int

const (
	duringMap phase = iota
	duringReduce
	finish
)

type Coordinator struct {
	nReduce        int
	MapTaskCnt     int
	ReduceTaskCnt  int
	phase          phase
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	Task           map[TaskType][]*Task
	mu             sync.Mutex
}

func (c *Coordinator) ResponseOneTask(args *NeedWorkArgs, reply *NeedWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceCnt = c.nReduce
	switch c.phase {
	case duringMap:
		reply.Task = c.FindAvailableTask(c.MapTaskChan)
		logger.Debug(logger.DInfo, "Response Map Task, file id: %v", reply.Task.TaskId)
	case duringReduce:
		reply.Task = c.FindAvailableTask(c.ReduceTaskChan)
		logger.Debug(logger.DInfo, "Response Reduce Task, reduce id: %v", reply.Task.ReduceId)
	case finish:
		reply.Task = &Task{Type: Done, TaskId: -1}
	}
	return nil
}

func (c *Coordinator) FindAvailableTask(taskChan chan *Task) *Task {
	if len(taskChan) > 0 {
		// 从channel中取出一个任务
		task := <-taskChan
		task.Status = Running
		task.StartTime = time.Now().Unix()
		return task
	}
	return &Task{Type: Done, TaskId: -1}
}

func (c *Coordinator) TaskDone(args *FinishWorkArgs, reply *FinishWorkReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		// 判断是否进行了crashHandle处理
		if c.Task[MapTask][args.Id].Status == Waiting {
			matchedFiles, _ := filepath.Glob(args.FileName)
			for _, file := range matchedFiles {
				os.Remove(file)
			}
			return nil
		}
		// 任务完成，将任务状态置为Finished
		c.Task[MapTask][args.Id].Status = Finished
		c.MapTaskCnt--
		logger.Debug(logger.DInfo, "map task done, map id: %v, map task cnt: %v,len map task chan: %v", args.Id, c.MapTaskCnt, len(c.MapTaskChan))
		if c.MapTaskCnt == 0 && len(c.ReduceTaskChan) == 0 {
			logger.Debug(logger.DInfo, "all map tasks are finished, switch to reduce phase")
			c.phase = duringReduce
			// 生成reduce任务，放入channel
			for i := 0; i < c.nReduce; i++ {
				t := NewReduceTask(ReduceTask, i)
				c.Task[ReduceTask] = append(c.Task[ReduceTask], t)
				c.ReduceTaskChan <- t
			}
		}
	case ReduceTask:
		// 判断是否进行了crashHandle处理
		if c.Task[ReduceTask][args.Id].Status == Waiting {
			os.Remove(args.FileName)
			return nil
		}
		// 任务完成，将任务状态置为Finished
		c.Task[ReduceTask][args.Id].Status = Finished
		c.ReduceTaskCnt--
		logger.Debug(logger.DInfo, "reduce task done, reduce id: %v, reduce task cnt: %v, len reduce task chan: %v", args.Id, c.ReduceTaskCnt, len(c.ReduceTaskChan))
		if c.ReduceTaskCnt == 0 && len(c.MapTaskChan) == 0 {
			logger.Debug(logger.DInfo, "all reduce tasks are finished, switch to finish phase")
			c.phase = finish
		}
	}
	return nil
}

func (c *Coordinator) CrashHandle() {
	for {
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		if c.phase == finish {
			c.mu.Unlock()
			return
		}

		// 检查是否有超时任务
		for _, tasks := range c.Task {
			for _, task := range tasks {
				cost := time.Now().Unix() - task.StartTime
				if task.Status == Running && cost > 10 {
					// 将超时任务重新放入channel
					logger.Debug(logger.DInfo, "task timeout, task type: %s, task id: %d, time cost: %d", task.Type, task.TaskId, cost)
					task.Status = Waiting
					if task.Type == MapTask {
						c.MapTaskChan <- task
					} else {
						c.ReduceTaskChan <- task
					}
				}
			}
		}
		c.mu.Unlock()
	}
}

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

func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == finish {
		logger.Debug(logger.DInfo, "All tasks are finished!")
		ret = true
	}

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskCnt:     len(files),
		ReduceTaskCnt:  nReduce,
		nReduce:        nReduce,
		phase:          duringMap,
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		Task:           make(map[TaskType][]*Task, len(files)+nReduce),
	}

	for i, file := range files {
		t := NewMapTask(MapTask, file, i)
		c.Task[MapTask] = append(c.Task[MapTask], t)
		c.MapTaskChan <- t
	}

	go c.CrashHandle()
	c.server()
	return &c
}

func NewMapTask(taskType TaskType, fileName string, taskId int) *Task {
	return &Task{
		Type:     taskType,
		FileName: fileName,
		TaskId:   taskId,
		Status:   Waiting,
	}
}

func NewReduceTask(taskType TaskType, reduceId int) *Task {
	return &Task{
		Type:     taskType,
		ReduceId: reduceId,
		Status:   Waiting,
	}
}
