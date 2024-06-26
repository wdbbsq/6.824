package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
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
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type EmptyArgs struct{}

const (
	NewTaskRpc       = "Coordinator.NewTask"
	MarkFinishedTask = "Coordinator.MarkFinishedTask"
	TaskErrRpc       = "Coordinator.HandleTaskErr"
)

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := os.Getpid()
	// to reuse workers
	for {
		reply := NewTaskReply{}
		// todo consider add worker id to distinguish each other
		if !call(NewTaskRpc, &NewTaskRequest{WorkerId: workerId}, &reply) {
			log.Println("RPC call failed: ", NewTaskRpc, ", reply: ", reply)
			return
		}
		switch reply.TaskType {
		case MapTask:
			doMap(mapf, reply, workerId)
		case ReduceTask:
			doReduce(reducef, reply, workerId)
		case EmptyTask:
			log.Printf("Worker-%v got nothing to do\n", workerId)
			if reply.Stage == EverythingOkay {
				return
			}
		default:
			log.Println("Unknown TaskType: ", reply.TaskType)
		}
		time.Sleep(time.Second)
	}
}

func doMap(mapf func(string, string) []KeyValue, t NewTaskReply, workerId int) {
	// read file
	file, err := os.Open(t.Filename)
	if err != nil {
		log.Printf("MapWorker-%v cannot open %v, %v\n", workerId, t.Filename, err.Error())
		request := HandleTaskErrRequest{
			MapTaskId:    t.TaskId,
			ReduceTaskId: -1,
		}
		call(TaskErrRpc, &request, &EmptyArgs{})
		return
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("MapWorker cannot read %v, %v\n", t.Filename, err.Error())
		return
	}
	kva := mapf(t.Filename, string(content))
	// spilt into buckets
	intermediate := make(map[int][]KeyValue)
	for _, kv := range kva {
		y := ihash(kv.Key) % t.NReduce
		if _, ok := intermediate[y]; !ok {
			intermediate[y] = make([]KeyValue, 0)
		}
		intermediate[y] = append(intermediate[y], kv)
	}
	// write into files
	for y, kvs := range intermediate {
		filename := fmt.Sprintf("./tmp/mr-%v-%v", t.TaskId, y)
		jsonFile, err := os.Create(filename)
		if err != nil {
			log.Printf("MapWorker cannot open %v, %v\n", filename, err.Error())
			return
		}
		encoder := json.NewEncoder(jsonFile)
		for _, kv := range kvs {
			encoder.Encode(kv)
		}
		jsonFile.Close()
	}
	// inform coordinator
	finishedTask := MarkFinishedTaskRequest{
		TaskType: t.TaskType,
		WorkerId: workerId,
		TaskId:   t.TaskId,
	}
	if !call(MarkFinishedTask, &finishedTask, &EmptyArgs{}) {
		return
	}
}

func doReduce(reducef func(string, []string) string, t NewTaskReply, workerId int) {
	defer func() {
		// clean up tmp files
	}()
	// read from files
	intermediate := make([]KeyValue, 0)
	for i := 0; i < t.NMap; i++ {
		// mr-MapTaskId-ReduceTaskId
		filename := fmt.Sprintf("./tmp/mr-%v-%v", i, t.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("ReduceWorker-%v cannot open %v, %v\n", workerId, filename, err.Error())
			// inform coordinator err occurred
			//request := HandleTaskErrRequest{
			//	MapTaskId:    i,
			//	ReduceTaskId: t.TaskId,
			//}
			//call(TaskErrRpc, &request, &EmptyArgs{})

			// ignore reduce map error as the MapReduce paper suggested
			continue
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// sort
	sort.Sort(ByKey(intermediate))
	// save result
	outName := fmt.Sprintf("mr-out-%v", t.TaskId)
	ofile, _ := os.Create(outName)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// inform coordinator
	finishedTask := MarkFinishedTaskRequest{
		TaskType: t.TaskType,
		WorkerId: workerId,
		TaskId:   t.TaskId,
	}
	if !call(MarkFinishedTask, &finishedTask, &EmptyArgs{}) {
		return
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	//log.Println("From", rpcname, "Response:", reply, " Err:", err)

	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
