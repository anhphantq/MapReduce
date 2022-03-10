package mr

import (
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
	mutexMap sync.Mutex
	mapTasks map[string]int // 1 is not assigned, 2 is assigned, 3 is done
	doneMapPhase int
	timeOutMapCh chan string

	// For reduce phase
	mutexReduce sync.Mutex
	reduceTasks map[int]int // 1 is not assigned, 2 is assigned, 3 is done
	doneReducePhase int
	timeOutReduceCh chan int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *EmptyArgs, reply *Reply) error {
	if (c.doneMapPhase < len(c.mapTasks)){

		c.mutexMap.Lock()
		
		for k, v := range c.mapTasks{
			if (v == 1){
				reply.taskType = 1
				reply.data = MapTask{filename: k}
				c.mapTasks[k] = 2

				go func (task string)  {
					time.Sleep(time.Second*10)

					c.mutexMap.Lock()

					if c.mapTasks[task] == 2 {
						c.mapTasks[task] = 1
					}

					c.mutexMap.Unlock()

					c.timeOutMapCh <- k
				}(k)
				
				c.mutexMap.Unlock()
				return nil
			}
		}

		c.mutexMap.Unlock()

		reply.taskType = 0
		return nil
	}

	if (c.doneMapPhase == len(c.mapTasks)){
		if (c.doneReducePhase < len(c.reduceTasks)){
			c.mutexReduce.Lock()

			for k, v := range c.reduceTasks{
				if (v == 1){
					reply.taskType = 1
					reply.data = ReduceTask{filename: k}
					c.reduceTasks[k] = 2
	
					go func (task int)  {
						time.Sleep(time.Second*10)
	
						c.mutexReduce.Lock()
	
						if c.reduceTasks[task] == 2 {
							c.reduceTasks[task] = 1
						}
	
						c.mutexReduce.Unlock()
	
						c.timeOutReduceCh <- k
					}(k)
					
					c.mutexMap.Unlock()
					return nil
				}
			}

			c.mutexReduce.Unlock()
		}
	}

	return nil
}

func (c *Coordinator) DoneMapTask(args *DoneMapArgs, reply *EmptyReply) error{
	c.mutexMap.Lock()

	if (c.mapTasks[args.filename] == 1) {
		return nil
	}

	c.mapTasks[args.filename] = 3 
	c.doneMapPhase++

	c.mutexMap.Unlock()

	return nil
}

func (c *Coordinator) DoneReduceTask(args *DoneReduceArgs, reply *EmptyReply) error{
	c.mutexReduce.Lock()

	if (c.reduceTasks[args.filename] == 1) {
		return nil
	}

	c.reduceTasks[args.filename] = 3 
	c.doneReducePhase++

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

func (c *Coordinator) isDone(args *EmptyArgs, reply *DoneReply) error{
	reply.isDone = ret
	return nil
}

func (c *Coordinator) Done() bool {
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func checkTimeOutMap(c *Coordinator){
	for {
		v, ok := <- c.timeOutMapCh

		if (!ok){
			break
		}
	
		c.mutexMap.Lock()

		c.mapTasks[v] = 1

		c.mutexMap.Unlock()
	}
}

func checkTimeOutReduce(c *Coordinator){
	for {
		v, ok := <- c.timeOutReduceCh

		if (!ok){
			break
		}
	
		c.mutexReduce.Lock()

		c.reduceTasks[v] = 1

		c.mutexReduce.Unlock()
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mutexMap: sync.Mutex{},
		doneMapPhase: 0,
		timeOutMapCh: make(chan string, len(files)),
	}
	c.server()

	// Your code here.

	// Map phase
	go checkTimeOutMap(&c)
	for c.doneMapPhase != len(files){
		time.Sleep(time.Second)
	}

	close(c.timeOutMapCh)

	// Reduce phase

	go checkTimeOutReduce(&c)
	for (c.doneReducePhase != nReduce){
		time.Sleep(time.Second)
	}

	close(c.timeOutReduceCh)

	ret = true

	return &c
}
