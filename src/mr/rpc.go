package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	EmptyTask
)

type Task struct {
	TaskType
	TaskId int
	// current worker id
	WorkerId int
	Filename string
	Timer    *time.Timer
}

type MarkFinishedTaskRequest struct {
	TaskType
	WorkerId int
	TaskId   int
}

type HandleTaskErrRequest struct {
	MapTaskId int
	// ReduceTaskId >= 0 means reduce-task needs re-build
	ReduceTaskId int
	WorkerId     int
}

type NewTaskRequest struct {
	WorkerId int
}

type NewTaskReply struct {
	TaskType
	TaskId   int
	Filename string
	Stage    stage
	NMap     int
	NReduce  int
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
