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
	nMap         int
	nReduce      int
	tasks        map[TaskType][]*Task
	workingTasks []*Task

	stage
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// alloc new Task to worker
func (c *Coordinator) NewTask(_ *EmptyArgs, reply *NewTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	switch c.stage {
	case Mapping:
		c.findAvailableTask(MapTask, reply)
	case Reducing:
		c.findAvailableTask(ReduceTask, reply)
	case EverythingOkay:
		reply.TaskType = NothingToDo
	}
	return nil
}

func (c *Coordinator) MarkFinishedTask(args *MarkFinishedTaskRequest, _ *EmptyArgs) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// delete from workingTasks
	j := 0
	var finishedTask *Task
	for _, t := range c.workingTasks {
		if t.TaskId != args.TaskId {
			c.workingTasks[j] = t
			j++
		} else {
			finishedTask = t
		}
	}
	c.workingTasks = c.workingTasks[:j]
	// todo is necessary to check expire here?
	select {
	case <-finishedTask.Timer.C:
		c.tasks[args.TaskType] = append(c.tasks[args.TaskType], finishedTask)
		log.Println("Task-", args.TaskType, "-", args.TaskId, " expired.")
	default:
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) checkExpiredTask() {
	count := 0
	for _, t := range c.workingTasks {
		select {
		case <-t.Timer.C:
			// add back to tasks
			c.tasks[t.TaskType] = append(c.tasks[t.TaskType], t)
		default:
			c.workingTasks[count] = t
			count++
		}
	}
	c.workingTasks = c.workingTasks[:count]
}

func (c *Coordinator) findAvailableTask(t TaskType, reply *NewTaskReply) {
	c.checkExpiredTask()
	if len(c.tasks[t]) == 0 && len(c.workingTasks) == 0 {
		switch t {
		case MapTask:
			c.stage = Reducing
			c.buildReduceTasks()
			log.Println("Map tasks are all done.")
		case ReduceTask:
			c.stage = EverythingOkay
			log.Println("Finished all tasks. Quitting...")
			return
		default:
			log.Println("Unknown Task type when findAvailableTask: ", t)
		}
	}
	if len(c.tasks[t]) == 0 {
		// no task in the task pool
		reply.TaskType = NothingToDo
		return
	}
	// copy fields not ptr
	targetTask := c.tasks[t][0]
	reply.TaskId = targetTask.TaskId
	reply.TaskType = t
	reply.Filename = targetTask.Filename
	// delete targetTask
	c.tasks[t] = c.tasks[t][1:]
	// start timing
	targetTask.Timer.Reset(TaskTimeout)
	c.workingTasks = append(c.workingTasks, targetTask)
	log.Println("Task-", targetTask.TaskType, "-", targetTask.TaskId, " allocated.")
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
	c.lock.Lock()
	ret = c.stage == EverythingOkay
	defer c.lock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:         len(files),
		nReduce:      nReduce,
		tasks:        make(map[TaskType][]*Task),
		workingTasks: make([]*Task, 0, len(files)),
		stage:        Mapping,
		lock:         sync.Mutex{},
	}
	// Your code here.
	c.lock.Lock()
	c.buildMapTasks(files)
	c.lock.Unlock()

	c.server()
	return &c
}

func (c *Coordinator) buildMapTasks(files []string) {
	tasks := make([]*Task, 0, len(files))
	for i, file := range files {
		tasks = append(tasks, &Task{
			TaskId:   i,
			TaskType: MapTask,
			Filename: file,
			Timer:    time.NewTimer(TaskTimeout),
		})
	}
	c.tasks[MapTask] = tasks
}

func (c *Coordinator) buildReduceTasks() {
	tasks := make([]*Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		tasks = append(tasks, &Task{
			TaskId:   i,
			TaskType: ReduceTask,
			Timer:    time.NewTimer(TaskTimeout),
		})
	}
	c.tasks[ReduceTask] = tasks
}
