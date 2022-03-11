package mr

import (
	"encoding/gob"
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

var path = "/home/phananhtq/Documents/cs6824/MapReduce"

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	name string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

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
		err = c.Call("Coordinator.IsDone", &args, &replyDone)
		if err != nil {
			log.Fatal(err.Error())
		}

		if replyDone.IsDone {
			break
		}

		err = c.Call("Coordinator.AssignTask", &args, &replyTask)

		if err != nil {
			log.Fatal(err.Error())
		}

		if replyTask.TaskType == 0 {
			time.Sleep(time.Second)
		} else if replyTask.TaskType == 1 {
			task := replyTask.Data.(MapTask)
			file, err := os.Open(path + "/" + task.Filename)

			if err != nil {
				log.Fatalf(err.Error())
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			file.Close()

			kva := mapf(task.Filename, string(content))

			var enc = make([]json.Encoder, nReduce)
			var files = make([]*os.File, nReduce)

			for i := 0; i < nReduce; i++ {
				files[i], _ = ioutil.TempFile("", "*.txt")

				enc[i] = *json.NewEncoder(files[i])
			}

			for i := 0; i < len(kva); i++ {
				tmp := ihash(kva[i].Key) % nReduce
				enc[tmp].Encode(kva[i])
			}

			for i := 0; i < nReduce; i++ {
				// os.Remove("../reduce_data/" + name + strconv.Itoa(task.idTask) + "-" + strconv.Itoa(i) + ".txt")
				os.Rename(files[i].Name(), path+"/reduce_data/"+name+"/"+strconv.Itoa(task.IdTask)+"-"+strconv.Itoa(i)+".txt")
			}

			err = c.Call("DoneMapTask", DoneMapArgs{Dirname: name, IdTask: task.IdTask}, EmptyReply{})

			if err != nil {
				log.Fatal(err.Error())
			}
		} else if replyTask.TaskType == 2 {
			task := replyTask.Data.(ReduceTask)
			for i := 0; i < task.NumOfMapTask; i++ {
				file, err := os.Open("../reduce_data/" + name + "/" + strconv.Itoa(i) + "-" + strconv.Itoa(task.IdTask) + ".txt")

				if err != nil {
					log.Fatal(err.Error(), "line 115")
				}

				var kva []KeyValue

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

		}
	}

}

func PingDoneMapTask(name string) {

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
