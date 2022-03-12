package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var ret = false

type Coordinator struct {
	// Your definitions here.

	// For map phase
	mutexMap     sync.Mutex
	mapTasks     map[int]int // 1 is not assigned, 2 is assigned, 3 is done
	doneMapPhase int
	timeOutMapCh chan int
	fileName     []string
	dirName      []string
	assignMapTask map[int]string
	
	// For reduce phase
	mutexReduce     sync.Mutex
	reduceTasks     map[int]int // 1 is not assigned, 2 is assigned, 3 is done
	doneReducePhase int
	timeOutReduceCh chan int
	assignReduceTask map[int]string

	doneMutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *AssignArgs, reply *Reply) error {
	c.mutexMap.Lock()
	if c.doneMapPhase < len(c.mapTasks) {

		for k, v := range c.mapTasks {
			if v == 1 {
				reply.TaskType = 1
				reply.Data = MapTask{Filename: c.fileName[k], IdTask: k}
				c.mapTasks[k] = 2
				c.assignMapTask[k] = args.Worker

				go func(task int) {
					time.Sleep(time.Second * 10)

					c.mutexMap.Lock()

					if c.mapTasks[task] == 2 {
						c.mapTasks[task] = 1
					}

					if c.mapTasks[task] == 3 {
						c.mutexMap.Unlock()
						return
					}

					c.mutexMap.Unlock()

					c.timeOutMapCh <- k
				}(k)

				c.mutexMap.Unlock()
				return nil
			}
		}

		c.mutexMap.Unlock()
		reply.TaskType = -1
		return nil
	}

	c.mutexMap.Unlock()

	if c.doneMapPhase == len(c.mapTasks) {
		c.mutexReduce.Lock()
		if c.doneReducePhase < len(c.reduceTasks) {

			for k, v := range c.reduceTasks {
				if v == 1 {
					reply.TaskType = 2
					reply.Data = ReduceTask{IdTask: k, NumOfMapTask: len(c.mapTasks), DirName: c.dirName}
					c.reduceTasks[k] = 2
					c.assignReduceTask[k] = args.Worker

					go func(task int) {
						time.Sleep(time.Second * 10)

						c.mutexReduce.Lock()

						if c.reduceTasks[task] == 2 {
							log.Print("SOS")
							c.reduceTasks[task] = 1
						}

						if c.reduceTasks[task] == 3 {
							c.mutexReduce.Unlock()
							return
						}

						c.mutexReduce.Unlock()

						c.timeOutReduceCh <- k
					}(k)

					c.mutexReduce.Unlock()
					return nil
				}
			}

			reply.TaskType = -1
			c.mutexReduce.Unlock()
			return nil
		}
		reply.TaskType = -1
		c.mutexReduce.Unlock()
	}

	//log.Print("OMG")
	reply.TaskType = -1
	return nil
}

func (c *Coordinator) DoneMapTask(args *DoneMapArgs, reply *EmptyReply) error {
	c.mutexMap.Lock()

	if c.mapTasks[args.IdTask] == 1 || c.mapTasks[args.IdTask] == 3 || c.assignMapTask[args.IdTask] != args.Worker{
		c.mutexMap.Unlock()
		return nil
	}

	c.mapTasks[args.IdTask] = 3
	c.dirName[args.IdTask] = args.Dirname
	c.doneMapPhase++
	c.mutexMap.Unlock()

	return nil
}

func (c *Coordinator) DoneReduceTask(args *DoneReduceArgs, reply *EmptyReply) error {
	c.mutexReduce.Lock()

	if c.reduceTasks[args.Filename] == 1 || c.reduceTasks[args.Filename] == 3 || c.assignReduceTask[args.Filename] != args.Worker {
		c.mutexReduce.Unlock()
		return nil
	}

	c.reduceTasks[args.Filename] = 3
	c.doneReducePhase++
	//log.Print(c.doneMapPhase)
	c.mutexReduce.Unlock()

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

func (c *Coordinator) IsDone(args *EmptyArgs, reply *DoneReply) error {
	c.doneMutex.Lock()
	reply.IsDone = ret
	c.doneMutex.Unlock()
	return nil
}

func (c *Coordinator) Done() bool {
	c.doneMutex.Lock()
	ans := ret
	c.doneMutex.Unlock()
	return ans
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func checkTimeOutMap(c *Coordinator) {
	for {
		v, ok := <-c.timeOutMapCh 

		if !ok {
			break
		}

		c.mutexMap.Lock()

		c.mapTasks[v] = 1

		c.mutexMap.Unlock()
	}
}

func checkTimeOutReduce(c *Coordinator) {
	for {
		v, ok := <-c.timeOutReduceCh

		if !ok {
			break
		}

		c.mutexReduce.Lock()

		c.reduceTasks[v] = 1

		c.mutexReduce.Unlock()
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	c := Coordinator{
		mutexMap:        sync.Mutex{},
		doneMapPhase:    0,
		timeOutMapCh:    make(chan int, len(files)),
		mutexReduce:     sync.Mutex{},
		doneReducePhase: 0,
		timeOutReduceCh: make(chan int, nReduce),
	}

	c.dirName = make([]string, len(files))
	c.fileName = make([]string, len(files))
	c.mapTasks = make(map[int]int)
	c.assignMapTask = make(map[int]string)
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = 1
		c.fileName[i] = files[i]
	}

	c.reduceTasks = make(map[int]int)
	c.assignReduceTask = make(map[int]string)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = 1
	}

	c.server()

	// Your code here.

	// Map phase
	go checkTimeOutMap(&c)
	for{
		c.mutexMap.Lock()
		if c.doneMapPhase == len(files){
			c.mutexMap.Unlock()
			break
		}
		c.mutexMap.Unlock()
		time.Sleep(time.Second)
	}

	close(c.timeOutMapCh)

	// Reduce phase

	go checkTimeOutReduce(&c)
	for {
		c.mutexReduce.Lock()
		if (c.doneReducePhase == nReduce){
			c.mutexReduce.Unlock()
			break
		}
		c.mutexReduce.Unlock()
		time.Sleep(time.Second)
	}

	close(c.timeOutReduceCh)

	c.doneMutex.Lock()
	ret = true
	c.doneMutex.Unlock()

	return &c
}
