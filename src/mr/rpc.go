package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// ApplyForTaskArgs
// 由 Worker 向 Coordinator 发起，申请一个新的 Task，同时汇报上一个运行完成的 Task
// Coordinator 接收到 RPC 请求后将同步阻塞，
// 直到有可用的 Task 分配给该 Worker 或整个 MR 作业已运行完成
type ApplyForTaskArgs struct {
	WorkerID string 
	PrevTaskType string 
	PrevTaskIndex int 
}

// ApplyForTaskReply 
// 新 Task 的类型及 Index。若为空则代表 MR 作业已完成，Worker 可退出
type ApplyForTaskReply struct {
	TaskType string 
	TaskIndex int 
	MapNum int 
	ReduceNum int 
	MapInputFile string 
}

// Add your RPC definitions here.
const (
	MAP = "MAP"
	REDUCE = "REDUCE"
)

type Task struct {
	Deadline time.Time
	Type string // Map or Reduce 
	Index int 
	MapInputFile string 
	WorkerID string 
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tempMapOutFile(worker string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}
func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}
func tempReduceOutFile(worker string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", worker, reduceIndex)
}
func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}