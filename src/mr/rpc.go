package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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

type RequestTaskArgs struct {
	Pid int
}

type RequestTaskReply struct {
	HasTask bool
	IsMap bool
	FileName string // might need to change this later
	TaskNumber int
	NReduce int
	IntermediateFiles []string // this is empty for map tasks
}

type FinishedTaskArgs struct {
	Pid int
	IsMap bool // if false, then the worker is reduce
	TaskNumber int
}

type FinishedTaskReply struct {
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
