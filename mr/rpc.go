package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type EmptyArgs struct {
}

type EmptyReply struct{
}

type DoneReply struct{
	isDone bool
}

type DoneMapArgs struct{
	filename string
}

type DoneReduceArgs struct{
	filename int
}

type Reply struct {
	taskType int // 0 is no job available, 1 is Map job, 2 is Reduce job, 3 is back up job
	data interface{}
}

type MapTask struct{
	filename string
}

type ReduceTask struct{
	filename int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
