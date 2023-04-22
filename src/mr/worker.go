package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i int, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i int, j int) bool {
	return a[i].Key < a[j].Key
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	workerId := os.Getpid()
	lastTaskId := -1
	lastTaskType := ""
	log.Printf("worker[%v] starting....", workerId)
	for {
		args := DispatchTaskArgs{
			WorkerId:     workerId,
			LastTaskId:   lastTaskId,
			LastTaskType: lastTaskType,
		}
		reply := DispatchTaskReply{}

		ok := call("Coordinator.DispatchTask", &args, &reply)

		if !ok {
			log.Fatalf("worker[%v] cannot call Coordinator.DispatchTask", workerId)
		}

		if reply.TaskStage == Map {
			doMapTask(reply.Filename, workerId, reply.TaskId, reply.NReduce, mapf)
		} else if reply.TaskStage == Reduce {
			doReduceTask(reply.Filename, workerId, reply.TaskId, reply.NMap, reducef)
		} else if reply.TaskStage == Done {
			break
		}

		//	if reply.TaskStage == Map || reply.TaskStage == Reduce {
		lastTaskId = reply.TaskId
		lastTaskType = reply.TaskStage
		//}
	}

}

func tempMapOutFilename(workerId int, taskId int, n int) string {
	return fmt.Sprintf("temp-mr-%v-%v-%v", workerId, taskId, n)
}

func mapOutFilename(taskId int, n int) string {
	return fmt.Sprintf("mr-%v-%v", taskId, n)
}

func tempReduceOutFilename(workerId int, taskId int) string {
	return fmt.Sprintf("temp-mr-out-%v-%v", workerId, taskId)
}

func reduceOutFilename(taskId int) string {
	return fmt.Sprintf("mr-out-%v", taskId)
}

func doMapTask(filename string, workerId int, taskId int, nReduce int, mapf func(string, string) []KeyValue) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("worker[%v] cannot open file[%v]", workerId, filename)
	}

	content, err := ioutil.ReadAll(f)

	kvs := mapf(filename, string(content))

	for i := 0; i < nReduce; i++ {
		f, err := os.Create(tempMapOutFilename(workerId, taskId, i))
		//fmt.Printf("%v\n", tempMapOutFilename(workerId, taskId, i))
		if err != nil {
			log.Fatalf("worker[%v] cannot create file[%v]", workerId, filename)
		}

		for _, kv := range kvs {
			if (ihash(kv.Key) % nReduce) == i {
				fmt.Fprintf(f, "%v\t%v\n", kv.Key, kv.Value)
			}
		}
		f.Close()
	}
}

func doReduceTask(filename string, workerId int, taskId int, nMap int, reducef func(string, []string) string) {
	var lines []string

	for i := 0; i < nMap; i++ {
		mapOutFile := mapOutFilename(i, taskId)
		f, err := os.Open(mapOutFile)
		if err != nil {
			log.Fatalf("worker[%v] cannot open file[%v]", workerId, mapOutFile)
		}

		content, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			log.Fatalf("worker[%v] cannot read file[%v]", workerId, mapOutFile)
		}

		lines = append(lines, strings.Split(string(content), "\n")...)
	}

	var kvs []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kvs = append(kvs, KeyValue{split[0], split[1]})
	}

	sort.Sort(ByKey(kvs))

	f, err := os.Create(tempReduceOutFilename(workerId, taskId))
	//	fmt.Printf("%v\n", tempReduceOutFilename(workerId, taskId))
	if err != nil {
		log.Fatalf("worker[%v] cannot create file[%v]", tempReduceOutFilename(workerId, taskId))
	}

	for i := 0; i < len(kvs); {
		var values []string
		values = append(values, kvs[i].Value)

		j := i + 1
		for ; j < len(kvs); j++ {
			if kvs[j].Key != kvs[i].Key {
				break
			}
			values = append(values, kvs[j].Value)
		}

		ret := reducef(kvs[i].Key, values)
		fmt.Fprintf(f, "%v %v\n", kvs[i].Key, ret)

		i = j
	}
	f.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
