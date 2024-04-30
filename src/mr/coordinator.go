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
	files        []string
	tasks        map[TaskType][]*Task
	workingTasks []*Task

	stage
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// alloc new Task to worker
func (c *Coordinator) NewTask(args *NewTaskRequest, reply *NewTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch c.stage {
	case Mapping:
		c.findAvailableTask(MapTask, args.WorkerId, reply)
	case Reducing:
		c.findAvailableTask(ReduceTask, args.WorkerId, reply)
	case EverythingOkay:
		reply.TaskType = EmptyTask
	}
	reply.Stage = c.stage
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) MarkFinishedTask(args *MarkFinishedTaskRequest, _ *EmptyArgs) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	finishedTask := c.deleteFromWorkingTasks(args.TaskId, args.TaskType)
	if finishedTask == nil {
		return nil
	}
	if args.WorkerId != finishedTask.WorkerId {
		log.Printf("Get reply from expired worker %v\n", args.WorkerId)
		return nil
	}
	// check expiration
	select {
	case <-finishedTask.Timer.C:
		c.append2Tasks(finishedTask)
		log.Println("Task-", args.TaskType, "-", args.TaskId, " expired.")
	default:
		log.Println("Finished Task: ", finishedTask)
	}
	// stop timer
	finishedTask.Timer.Stop()
	return nil
}

// HandleTaskErr would reschedule working tasks
// when map or reduce workers having trouble
func (c *Coordinator) HandleTaskErr(args *HandleTaskErrRequest, _ *EmptyArgs) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// rebuild map task
	mapTask := c.deleteFromWorkingTasks(args.MapTaskId, MapTask)
	if mapTask == nil {
		return nil
	}
	c.append2Tasks(mapTask)

	c.stage = Mapping

	if args.ReduceTaskId >= 0 {
		// rebuild reduce task
		reduceTask := c.deleteFromWorkingTasks(args.ReduceTaskId, ReduceTask)
		if reduceTask == nil {
			return nil
		}
		c.append2Tasks(reduceTask)
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

func (c *Coordinator) deleteFromWorkingTasks(taskId int, taskType TaskType) *Task {
	j := 0
	var finishedTask *Task
	for _, t := range c.workingTasks {
		if t.TaskId == taskId && t.TaskType == taskType {
			finishedTask = t
		} else {
			c.workingTasks[j] = t
			j++
		}
	}
	if finishedTask == nil {
		log.Printf("There has no task with id %v\n", taskId)
		// which means the specific task is done but files missing,
		// so we rebuild one
		//finishedTask = c.TaskFactory(taskId, taskType)
	} else {
		c.workingTasks = c.workingTasks[:j]
		finishedTask.Timer.Stop()
	}
	return finishedTask
}

func (c *Coordinator) append2Tasks(task *Task) {
	for _, t := range c.tasks[task.TaskType] {
		if t.TaskId == task.TaskId {
			return
		}
	}
	c.tasks[task.TaskType] = append(c.tasks[task.TaskType], task)
}

func (c *Coordinator) checkExpiredTask() {
	count := 0
	for _, t := range c.workingTasks {
		select {
		case <-t.Timer.C:
			t.Timer.Stop()
			// add back to tasks
			c.append2Tasks(t)
		default:
			c.workingTasks[count] = t
			count++
		}
	}
	c.workingTasks = c.workingTasks[:count]
}

func (c *Coordinator) findAvailableTask(t TaskType, workerId int, reply *NewTaskReply) {
	c.checkExpiredTask()
	if len(c.tasks[t]) == 0 && len(c.workingTasks) == 0 {
		switch t {
		case MapTask:
			c.stage = Reducing
			c.buildTasks(ReduceTask)
			log.Println("#### Map tasks finished.")
			t = ReduceTask
		case ReduceTask:
			c.stage = EverythingOkay
			log.Println("#### All tasks finished. Quitting...")
			return
		default:
			log.Println("Unknown Task type when findAvailableTask: ", t)
		}
	}
	if len(c.tasks[t]) == 0 {
		// no task in the task pool
		reply.TaskType = EmptyTask
		return
	}
	targetTask := c.tasks[t][0]
	targetTask.WorkerId = workerId

	// copy fields not ptr
	reply.TaskId = targetTask.TaskId
	reply.TaskType = t
	reply.Filename = targetTask.Filename

	// delete targetTask from task pool
	c.tasks[t] = c.tasks[t][1:]

	// start timing
	targetTask.Timer.Reset(TaskTimeout)
	c.workingTasks = append(c.workingTasks, targetTask)
	log.Printf("Task-%v-%v allocated to worker-%v\n", targetTask.TaskType, targetTask.TaskId, workerId)
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
		files:        files,
		tasks:        make(map[TaskType][]*Task),
		workingTasks: make([]*Task, 0, MaxInt(len(files), nReduce)),
		stage:        Mapping,
		lock:         sync.Mutex{},
	}
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	// clean tmp dir
	err := os.RemoveAll("./tmp")
	if err != nil {
		log.Println("Trouble with clean tmp files: ", err)
	}
	err = os.Mkdir("./tmp", 0777)
	if err != nil {
		log.Println("Fail to create dir: ", err)
		return nil
	}

	c.buildTasks(MapTask)

	c.server()
	log.Println("Coordinator started.")
	return &c
}

func (c *Coordinator) TaskFactory(taskId int, taskType TaskType) *Task {
	var filename string
	if taskType == MapTask {
		filename = c.files[taskId]
	}
	return &Task{
		TaskType: taskType,
		TaskId:   taskId,
		WorkerId: -1,
		Filename: filename,
		Timer:    time.NewTimer(TaskTimeout),
	}
}

func (c *Coordinator) buildTasks(taskType TaskType) {
	var size int
	switch taskType {
	case MapTask:
		size = c.nMap
	case ReduceTask:
		size = c.nReduce
	default:
		panic("unhandled default case")
	}
	tasks := make([]*Task, size)
	for i := 0; i < size; i++ {
		tasks[i] = c.TaskFactory(i, taskType)
		tasks[i].Timer.Stop()
	}
	c.tasks[taskType] = tasks
}
