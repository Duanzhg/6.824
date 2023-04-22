package mr

import (
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Map    = "Map"
	Reduce = "Reduce"
	Wait   = "Wait"
	Done   = "Done"
)

type Task struct {
	taskId   int
	workerId int
	filename string
	deadline time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu       sync.Mutex
	files    []string
	stage    string
	nMap     int
	nReduce  int
	tasks    map[int]Task
	toDoTask chan Task
}

func (c *Coordinator) schedule() {
	if c.stage == Map {
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				taskId:   i,
				workerId: -1,
			}
			c.tasks[i] = task
			c.toDoTask <- task
		}
		c.stage = Reduce
	} else if c.stage == Reduce {
		c.stage = Done
		close(c.toDoTask)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DispatchTask(args *DispatchTaskArgs, reply *DispatchTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	if args.LastTaskId != -1 {
		if args.LastTaskType == Map {
			for i := 0; i < c.nReduce; i++ {
				err := os.Rename(tempMapOutFilename(args.WorkerId, args.LastTaskId, i), mapOutFilename(args.LastTaskId, i))
				if err != nil {
					log.Fatalf("coordinator cannot rename file[%v] to file[%v]",
						tempMapOutFilename(args.WorkerId, args.LastTaskId, i), mapOutFilename(args.LastTaskId, i))
				}
			}
		} else if args.LastTaskType == Reduce {
			//	fmt.Printf("%v\n", reduceOutFilename(args.LastTaskId))
			err := os.Rename(tempReduceOutFilename(args.WorkerId, args.LastTaskId), reduceOutFilename(args.LastTaskId))
			if err != nil {
				log.Fatalf("coordinator cannot rename file[%v] to file[%v]",
					tempReduceOutFilename(args.WorkerId, args.LastTaskId), reduceOutFilename(args.LastTaskId))
			}
		}

		log.Printf("%v task %v done", args.LastTaskType, args.LastTaskId)
		delete(c.tasks, args.LastTaskId)
		if len(c.tasks) == 0 {
			c.schedule()
		}

	}

	c.mu.Unlock()
	task := <-c.toDoTask
	c.mu.Lock()
	//	log.Printf("%v", task.taskId)
	task.deadline = time.Now().Add(10 * time.Second)
	task.workerId = args.WorkerId
	c.tasks[task.taskId] = task

	reply.TaskId = task.taskId
	reply.Filename = task.filename
	reply.TaskStage = c.stage
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = (c.stage == Done)
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		stage:    Map,
		nMap:     len(files),
		nReduce:  nReduce,
		tasks:    make(map[int]Task),
		toDoTask: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	for i, file := range files {
		task := Task{
			taskId:   i,
			workerId: -1,
			filename: file,
		}

		c.tasks[i] = task
		c.toDoTask <- task
	}

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.mu.Lock()
			for _, task := range c.tasks {
				if task.workerId != -1 && time.Now().After(task.deadline) {
					log.Printf("coordinator recycle task[%v]", task.taskId)
					task.workerId = -1
					c.tasks[task.taskId] = task
					c.toDoTask <- task
				}
			}
			c.mu.Unlock()
		}
	}()

	c.server()
	return &c
}
