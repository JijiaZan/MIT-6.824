package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady int = iota
	TaskStatusQueue     // 进入等待队列
	TaskStatusRunning
	TaskStatusFinish
	TaskStatusErr
)

const (
	MaxTaskRuntime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 5
)

type TaskStat struct {
	Status    int
	WorkerID  int
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	files     []string
	nReduce   int
	taskPhase TaskPhase
	taskStats []TaskStat
	mu        sync.Mutex
	done      bool
	workerSeq int
	taskCh    chan Task
}

//
// Done main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

//
// MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files

	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}

	m.initMapTask()
	go m.tickSchedule()
	m.server()
	DPrintf("master init")
	return &m
}

// TestRPC 测试
// RPC Service
func (m *Master) TestRPC(args *TestArgs, reply *TestReply) error {
	fmt.Println("成功啦")
	reply.Res = args.X + args.Y
	return nil
}

// RegWorker 分配一个id(seq)给一个worker
// RPC Service
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq++
	reply.WorkerID = m.workerSeq
	return nil
}

// GetOneTask give an task to worker when worker calls requestTask()
// RPC Service
func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task

	if task.Alive {
		m.regTask(args, &task)
	}
	DPrintf("in get one task, args: %+v, reply: %+v", args, reply)
	return nil
}

// ReportTask handle the result of a task
// RPC service
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	DPrintf("get task report: %+v, taskPhase: %+v", args, m.taskPhase)
	if m.taskPhase != args.Phase || args.WorkerID != m.taskStats[args.Seq].WorkerID {
		return nil
	}
	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}
	go m.schedule()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// regtask 更新已经分配给worker的任务的status
func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		panic("request task phase not match")
	}

	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerID = args.WorkerID
	m.taskStats[task.Seq].StartTime = time.Now()
}

// 初始化map任务
func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
}

// 在map任务都完成后，初始化reduce任务
func (m *Master) initReduceTask() {
	DPrintf("init ReduceTask")
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	allFinish := true
	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady:
			allFinish = false
			m.taskCh <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if time.Now().Sub(t.StartTime) > MaxTaskRuntime {
				m.taskStats[index].Status = TaskStatusQueue // 运行超时，进入队列重新分配
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			m.taskStats[index].Status = TaskStatusQueue // 运行出错，进入队列重新分配
			m.taskCh <- m.getTask(index)
		default:
			panic("t.status err")
		}
	}

	if allFinish == true {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMap:     len(m.files),
		Seq:      taskSeq,
		Phase:    m.taskPhase,
		Alive:    true,
	}
	DPrintf("m: %+v, taskSeq: %d, lenFiles: %d, lenTaskStatus: %d", m, taskSeq, len(m.files), len(m.taskStats))
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}
