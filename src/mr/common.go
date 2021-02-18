package mr

import (
	"fmt"
	"log"
)

// DEBUG indicate the debug mod
const DEBUG = true

type TaskPhase int

// MapPhase & ReducePhase
const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

// Task to be completed
type Task struct {
	FileName string
	NReduce  int
	NMap     int
	Seq      int       // index
	Phase    TaskPhase // map or reduce
	Alive    bool
}

// DPrintf I don't know why to use this
func DPrintf(format string, v ...interface{}) {
	if DEBUG {
		log.Printf(format+"\n", v...)
	}
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func outputName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
