package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskStatus int

const (
	Waiting TaskStatus = iota // wait for getting
	Running
	Finished
)

type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	Done       TaskType = "done"
)

type Task struct {
	FileName  string
	TaskId    int
	ReduceId  int
	Status    TaskStatus
	Type      TaskType
	StartTime int64
}

type NeedWorkArgs struct{}
type NeedWorkReply struct {
	Task      *Task
	ReduceCnt int
}
type FinishWorkArgs struct {
	Id       int
	TaskType TaskType
	FileName string
}
type FinishWorkReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
