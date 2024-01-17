package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type Task struct {
	Type      string
	FileName  string
	TaskId    int
	ReduceId  int
	Status    TaskStatus
	StartTime int64
}

type NeedWorkArgs struct{}
type NeedWorkReply struct {
	Task      Task
	ReduceCnt int
}
type FinishWorkArgs struct {
	TaskId   int
	ReduceId int
	Type     TaskType
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
