package mr

import (
	"fmt"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"time"
	"sync"
)

const taskTimeoutTime float64 = 10.0

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	inputFiles []string
	nReduce int

	mapTaskToWorker map[int]*runningWorkerProcess
	finishedMapTasks map[int]bool // treat as set
	nextUnassignedMapTask int

	reduceTaskToWorker map[int]*runningWorkerProcess
	finishedReduceTasks map[int]bool // treat as set
	nextUnassignedReduceTask int
}

type runningWorkerProcess struct {
	pid int
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// handler for a task request from a worker thread
func (c *Coordinator) TaskRequestHandler(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	if len(c.finishedMapTasks) < len(c.inputFiles) { // map tasks not completed
		hasTimedOutMapTask := false
		for k, v := range c.mapTaskToWorker {
			duration := time.Since(v.startTime)
			if duration.Seconds() >= taskTimeoutTime {
				v.pid = args.Pid
				v.startTime = time.Now()
				setRequestTaskReply(reply, true, true, c.inputFiles[k], k,
					 				c.nReduce, nil)
				hasTimedOutMapTask = true
				break
			} 
		}
		if !hasTimedOutMapTask {
			if c.nextUnassignedMapTask < len(c.inputFiles) {
				c.mapTaskToWorker[c.nextUnassignedMapTask] = &runningWorkerProcess{pid: args.Pid, startTime: time.Now()}
				setRequestTaskReply(reply, true, true,
					 				c.inputFiles[c.nextUnassignedMapTask],
								    c.nextUnassignedMapTask, c.nReduce,
									nil)
				c.nextUnassignedMapTask++ 
			} else { // all map task assigned and currently working
				setRequestTaskReply(reply, false, false, "", -1, -1, nil)
			}
		}
	} else { // all map tasks completed, try to assign reduce task
		hasTimedOutReduceTask := false
		for k, v := range c.reduceTaskToWorker {
			duration := time.Since(v.startTime)
			if duration.Seconds() >= taskTimeoutTime {
				v.pid = args.Pid
				v.startTime = time.Now()
				setRequestTaskReply(reply, true, false, "", k, c.nReduce,
				 					generateIntermediateFileNames(len(c.inputFiles), k))
				hasTimedOutReduceTask = true
				break
			} 
		}
		if !hasTimedOutReduceTask {
			if c.nextUnassignedReduceTask < c.nReduce {
				c.reduceTaskToWorker[c.nextUnassignedReduceTask] = &runningWorkerProcess{pid: args.Pid, startTime: time.Now()}
				setRequestTaskReply(reply, true, false, "",
								    c.nextUnassignedReduceTask, c.nReduce,
									generateIntermediateFileNames(len(c.inputFiles), c.nextUnassignedReduceTask))
				c.nextUnassignedReduceTask++ 
			} else { // all reduce tasks assigned and currently working
				setRequestTaskReply(reply, false, false, "", -1, -1, nil)
			}
		}
	}
	return nil
}

func setRequestTaskReply(reply *RequestTaskReply, HasTask, IsMap bool,
	 					 FileName string, TaskNumber, NReduce int,
						 IntermediateFiles []string) {
	reply.HasTask = HasTask
	reply.IsMap = IsMap
	reply.FileName = FileName
	reply.TaskNumber = TaskNumber
	reply.NReduce = NReduce
	reply.IntermediateFiles = IntermediateFiles
}

func generateIntermediateFileNames(nMap, reduceTaskNumber int) []string {

	fileNames := make([]string, 0)
	for i := 0; i < nMap; i++ {
		fileNames = append(fileNames, fmt.Sprintf("mr-%v-%v", i, reduceTaskNumber))
	}

	return  fileNames
}

// handler for when worker sends an rpc notifying coordinator that it has  
// finished its task
func (c *Coordinator) TaskFinishedHandler(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	if args.IsMap {
		if _, ok := c.mapTaskToWorker[args.TaskNumber]; ok {
			delete(c.mapTaskToWorker, args.TaskNumber)
		}
		c.finishedMapTasks[args.TaskNumber] = true
	} else {
		if _, ok := c.reduceTaskToWorker[args.TaskNumber]; ok {
			delete(c.reduceTaskToWorker, args.TaskNumber)
		}
		c.finishedReduceTasks[args.TaskNumber] = true
	}

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
	c.mu.Lock()
    defer c.mu.Unlock()
	ret := false

	// Your code here.
	if len(c.finishedMapTasks) == len(c.inputFiles) && len(c.finishedReduceTasks) == c.nReduce {
		ret = true
	}


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.nReduce = nReduce

	c.mapTaskToWorker = make(map[int]*runningWorkerProcess)
	c.finishedMapTasks = make(map[int]bool)
	c.nextUnassignedMapTask = 0

	c.reduceTaskToWorker = make(map[int]*runningWorkerProcess)
	c.finishedReduceTasks = make(map[int]bool)
	c.nextUnassignedReduceTask = 0


	c.server()
	return &c
}
