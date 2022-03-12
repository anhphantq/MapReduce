package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	args := AssignArgs{Worker: name}
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
		//fmt.Print(replyTask.TaskType, " ")

		if err != nil {
			log.Fatal(err.Error())
		}

		if replyTask.TaskType == -1 {
			time.Sleep(time.Second)
		} else if replyTask.TaskType == 1 {
			task := replyTask.Data.(MapTask)
			file, err := os.Open(Path + "/" + task.Filename)

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
				os.Rename(files[i].Name(), Path+"/reduce_data/"+name+"/"+strconv.Itoa(task.IdTask)+"-"+strconv.Itoa(i)+".txt")
			}

			err = c.Call("Coordinator.DoneMapTask", DoneMapArgs{Dirname: name, IdTask: task.IdTask, Worker: name}, &EmptyReply{})

			if err != nil {
				log.Fatal(err.Error())
			}
		} else if replyTask.TaskType == 2 {
			task := replyTask.Data.(ReduceTask)
			var kva []KeyValue
			for i := 0; i < task.NumOfMapTask; i++ {
				file, err := os.Open(Path + "/reduce_data/" + task.DirName[i] + "/" + strconv.Itoa(i) + "-" + strconv.Itoa(task.IdTask) + ".txt")

				if err != nil {
					log.Fatal(err.Error(), "line 115")
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			oname := "mr-out-" + strconv.Itoa(task.IdTask)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()

			err = c.Call("Coordinator.DoneReduceTask", DoneReduceArgs{Filename: task.IdTask, Worker: name}, &EmptyReply{})

			if err != nil {
				log.Fatal(err.Error())
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
