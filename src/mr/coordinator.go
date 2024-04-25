package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TaskTimeout = 10 * time.Second

type stage int

const (
	Mapping stage = iota
	Reducing
	EverythingOkay
)

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	files        []string
	tasks        map[taskType]chan *task
	intermediate string

	stage
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// alloc new task to worker
func (c *Coordinator) NewTask(_ struct{}, reply *task) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch c.stage {
	case Mapping:
		c.findAvailableTask(MapTask)
		reply.taskType = MapTask
		reply.hasExpired = time.After(TaskTimeout)
	case Reducing:
		reply.taskType = ReduceTask
	case EverythingOkay:
		reply.taskType = NothingToDo
	}
	return nil
}

// store result of map task
func (c *Coordinator) StoreIntermediate(_ struct{}, reply *task) error {
	return nil
}

func (c *Coordinator) ReduceDone(_ struct{}, reply *task) error {
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) findAvailableTask(t taskType) (reply *task) {
	// todo check any working task has expired

	if len(c.tasks[t]) == 0 {
		switch t {
		case MapTask:
			c.stage = Reducing
		case ReduceTask:
			c.stage = EverythingOkay
		case NothingToDo:
		}
		return
	}
	reply = <-c.tasks[t]
	return
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.buildMapTasks(files)

	c.server()
	return &c
}

func (c *Coordinator) buildMapTasks(files []string) {
	// todo buffered channel or not?
	taskCh := make(chan *task, len(files))
	for _, file := range files {
		taskCh <- &task{
			taskType:   MapTask,
			filename:   file,
			hasExpired: nil,
		}
	}
	c.tasks[MapTask] = taskCh
}
