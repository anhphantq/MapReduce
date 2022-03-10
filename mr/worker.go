package mr

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var nReduce = 10

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



var path = "/Users/anhphantq/Desktop/CS6824/MapReduce/"
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	args := EmptyArgs{}
	replyDone := DoneReply{}
	replyTask := Reply{}

	

	for {
		err = c.Call("isDone", args, replyDone)
		if err != nil{
			log.Fatal("Something went wrong line 55")
		}

		err = c.Call("AssignTask", args, &replyTask)

		if err != nil{
			log.Fatal("Something went wrong line 62")
		}

		if (replyTask.taskType == 0){
			time.Sleep(time.Second)
		} else if (replyTask.taskType == 1){
			task := replyTask.data.(MapTask)
			file, err := os.Open(path + "map_data/" + task.filename)

			if err != nil {
				log.Fatalf("cannot open %v", task.filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.filename)
			}
			file.Close()
			
			kva := mapf(task.filename, string(content))

			var enc []json.Encoder
			var files []*os.File
			
			for i := 0; i < nReduce; i++{
				files[i], _ = ioutil.TempFile("../reduce_data", "*.txt")

				enc[i] = *json.NewEncoder(files[i])
			}

			for i := 0; i < len(kva); i++{
				tmp := ihash(kva[i].Key) % nReduce
				enc[tmp].Encode(kva[i])
			}

			for i := 0; i < nReduce; i++{
				os.Remove("../reduce_data/" + task.filename + "-" + strconv.Itoa(i) + ".txt")
				os.Rename("../reduce_data/" + files[i].Name(), "../reduce_data/" + task.filename + "-" + strconv.Itoa(i) + ".txt")
			}
		} else if (replyTask.taskType == 2){
			for ()
		}
	}

}

// func isDone() bool{
// 	sockname := coordinatorSock()
// 	c, err := rpc.DialHTTP("unix", sockname)
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}
// 	defer c.Close()

// 	args := EmptyArgs{}
// 	reply := DoneReply{}

// 	err = c.Call("isDone", args, reply)
// 	if err == nil {
// 		return reply.isDone
// 	}

// 	fmt.Println(err)
// 	return false
// }

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// func call(rpcname string, args interface{}, reply interface{}) bool {
// 	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
// 	sockname := coordinatorSock()
// 	c, err := rpc.DialHTTP("unix", sockname)
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}
// 	defer c.Close()

// 	err = c.Call(rpcname, args, reply)
// 	if err == nil {
// 		return true
// 	}

// 	fmt.Println(err)
// 	return false
// }
