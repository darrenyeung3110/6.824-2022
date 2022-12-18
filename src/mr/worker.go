package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"errors"
	"time"
	"io/ioutil"
	"encoding/json"
	"sort"
)



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
	// CallExample()

	for {
		requestTaskReply, err := requestTaskCall()

		// if there was an error, we can assume that the coodinator is done
		if err != nil {
			break
		}

		if !requestTaskReply.HasTask {
			time.Sleep(1 * time.Second)
		} else {
			if requestTaskReply.IsMap {
				handleMap(requestTaskReply, mapf)
			} else {
				handleReduce(requestTaskReply, reducef)
			}
		}
	}
}

func handleMap(requestTaskReply *RequestTaskReply,
	 		   mapf func(string, string) []KeyValue) {
	bytes, _ := ioutil.ReadFile(requestTaskReply.FileName)
	contents := string(bytes)
	keyValues := mapf(requestTaskReply.FileName, contents)
	writeIntermediateKeyValues(keyValues, requestTaskReply.TaskNumber,
								requestTaskReply.NReduce)	

	taskFinishedCall(os.Getpid(), true, requestTaskReply.TaskNumber)
}

// takes in a slice of KeyValues and writes them to 
// "mp-taskNumber-hash(key)%nReduce". Used for a map task
func writeIntermediateKeyValues(keyValues []KeyValue, taskNumber, nReduce int) {

	sortedKeyValues := sortKeyValues(keyValues)
	tempFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		tempFiles[i], _ = os.CreateTemp("", fmt.Sprintf("Tempfile: %v", i))
	}
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		encoders[i] = json.NewEncoder(tempFiles[i])
	}

	for _, kv := range sortedKeyValues {
		idx := ihash(kv.Key)%nReduce
		encoders[idx].Encode(&kv)
	}

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", taskNumber, i)
		os.Rename(tempFiles[i].Name(), fileName)
	}
}

func handleReduce(requestTaskReply *RequestTaskReply,
				  reducef func(string, []string) string) {

	kva := make([]KeyValue, 0)

	for _, v := range requestTaskReply.IntermediateFiles {
		file, _ := os.Open(v)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	if len(kva) > 0 {
		reducedKeyValues := getReduceOutput(kva, reducef)
		writeReduceOutput(reducedKeyValues, requestTaskReply.TaskNumber)
	}

	taskFinishedCall(os.Getpid(), false, requestTaskReply.TaskNumber)
}

func getReduceOutput(intermediateKeyValues []KeyValue,
	 				 reducef func(string, []string) string) []KeyValue {

	output := make([]KeyValue, 0)
	sorted := sortKeyValues(intermediateKeyValues)

	sl := make([]string, 0)
	previous := sorted[0].Key
	for _, kv := range sorted {
		if kv.Key == previous {
			sl = append(sl,kv.Value)
		} else {
			value := reducef(previous, sl)
			output = append(output, KeyValue{Key: previous, Value: value})
			sl = nil
			sl = append(sl, kv.Value)
			previous = kv.Key
		}
	}

	// last distinct key, how to bypass this last check?
	value := reducef(previous, sl)
	output = append(output, KeyValue{Key: previous, Value: value})

	return output
}

func writeReduceOutput(keyValues []KeyValue, taskNumber int) {
	tmpfile, _ := os.CreateTemp("", "TempFile")
	for _, kv := range keyValues {
		tmpfile.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
	}
	os.Rename(tmpfile.Name(), fmt.Sprintf("mr-out-%v", taskNumber))
}

func sortKeyValues(keyValues []KeyValue) (sortedKeyValues []KeyValue) {
	sortedKeyValues = make([]KeyValue, len(keyValues))
	copy(sortedKeyValues, keyValues)
	sort.Slice(sortedKeyValues, func(i, j int) bool {
		return sortedKeyValues[i].Key < sortedKeyValues[j].Key
	})
	return sortedKeyValues
}

func requestTaskCall() (*RequestTaskReply, error) {

	args := RequestTaskArgs{}
	args.Pid = os.Getpid()

	reply := RequestTaskReply{}

	ok := call("Coordinator.TaskRequestHandler", &args, &reply)
	if ok {
		/**
			fmt.Printf("reply.HasTask %v, reply.IsMap %v reply.FileName %v reply.TaskNumber %v reply.NReduce %v\n",
					reply.HasTask, reply.IsMap, reply.FileName, reply.TaskNumber,
					reply.NReduce)
		*/
		return &reply, nil
	} else {
		fmt.Printf("task request call failed!\n")
		err := fmt.Sprintf("task reqeuest call failed\n")
		return &reply, errors.New(err)
	}
}

func taskFinishedCall(pid int, isMap bool, taskNumber int) (*FinishedTaskReply, error) {

	args := FinishedTaskArgs{pid, isMap, taskNumber}
	reply := FinishedTaskReply{}

	ok := call("Coordinator.TaskFinishedHandler", &args, &reply)
	if ok {
		return &reply, nil
	} else {
		fmt.Printf("task finish call failed!\n")
		err := fmt.Sprintf("task finish call failed\n")
		return &reply, errors.New(err)
	}
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
