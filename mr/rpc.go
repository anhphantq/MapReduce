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

//"/home/phananhtq/Documents/cs6824/MapReduce"
var Path = "/Users/anhphantq/Desktop/CS6824/MapReduce"

type EmptyArgs struct {
}

type EmptyReply struct {
}

type AssignArgs struct {
	Worker string
}

type DoneReply struct {
	IsDone bool
}

type DoneMapArgs struct {
	IdTask  int
	Dirname string
	Worker string
}

type DoneReduceArgs struct {
	Filename int
	Worker string
}

type dt interface {
}

type Reply struct {
	TaskType int // 0 is no job available, 1 is Map job, 2 is Reduce job, 3 is back up job
	Data     dt
}

type MapTask struct {
	Filename string
	IdTask   int
}

type ReduceTask struct {
	IdTask       int
	NumOfMapTask int
	DirName      []string
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
