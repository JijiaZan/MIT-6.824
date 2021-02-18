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

type RegisterArgs struct{}

type RegisterReply struct {
	WorkerID int
}

// TaskArgs is used to ask master for a task
type TaskArgs struct {
	WorkerID int
}

type TaskReply struct {
	Task *Task
}

type ReportTaskArgs struct {
	Done bool
	Seq int
	Phase TaskPhase
	WorkerID int
}

type ReportTaskReply struct {}

type TestArgs struct {
	X int
	Y int
}

type TestReply struct {
	Res int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
