package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex
	stage string // Map or Reduce, 当前作业阶段，为空表示可以退出
	nMap int 
	nReduce int 
	tasks map[string]Task 
	availableTasks chan Task 
}

// Your code here -- RPC handlers for the worker to call.

// 由 Worker 调用向 Coordinator 申请新的 Task 处理函数
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	// Worker 请求新任务时需要告知上一个任务是什么，如果不为空，说明确实完成了上一个任务
	if args.PrevTaskType != "" {
		c.lock.Lock() 
		prevTaskID := GenTaskID(args.PrevTaskType, args.PrevTaskIndex)
		// 判断是否有这个任务并且确实是由这个 Worker 完成的
		if task, exist := c.tasks[prevTaskID]; exist && args.WorkerID == task.WorkerID {
			log.Printf("mark %s task %d as completed by worker %s\n", 
				task.Type, task.Index, args.WorkerID)
			// 将 Worker 产出的临时文件转换成最终产出文件
			if args.PrevTaskType == MAP {
				for reduceIndex := 0; reduceIndex < c.nReduce; reduceIndex++ {
					err := os.Rename(tempMapOutFile(args.WorkerID, 
						args.PrevTaskIndex, reduceIndex), 
						finalMapOutFile(args.PrevTaskIndex, reduceIndex))

					if err != nil {
						log.Fatalf("failed to rename %s: %e", tempMapOutFile(
							args.WorkerID, args.PrevTaskIndex, reduceIndex), err)
					}
				}
			} else if args.PrevTaskType == REDUCE {
				err := os.Rename(tempReduceOutFile(args.WorkerID, args.PrevTaskIndex),
								 finalReduceOutFile(args.PrevTaskIndex))
				if err != nil {
					log.Fatalf("failed to rename %s: %e", tempReduceOutFile(
						args.WorkerID, args.PrevTaskIndex), err)
				}
			}
			delete(c.tasks, prevTaskID)

			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}

	task, ok := <- c.availableTasks
	if !ok {     // channel 关闭，代表所有 MR 作业已经完成，通知 Worker 退出
		return nil 
	}
	c.lock.Lock() 
	defer c.lock.Unlock()
	log.Printf("assign %s task %d to worker: %s\n", 
		task.Type, task.Index, args.WorkerID)
		
	task.WorkerID = args.WorkerID 
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task 
	reply.MapInputFile = task.MapInputFile 
	reply.TaskIndex = task.Index 
	reply.TaskType = task.Type 
	reply.MapNum = c.nMap 
	reply.ReduceNum = c.nReduce

	return nil 
}

// 负责从 Map 阶段切换到 Reduce 阶段，或是从 Reduce 阶段切换到全部完成阶段
func (c *Coordinator) transit() {
	if c.stage == MAP {
		log.Printf("all map tasks are completed\n")
		c.stage = REDUCE

		for i := range c.nReduce {
			task := Task {
				Type: REDUCE,
				Index: i,
			}
			c.tasks[GenTaskID(REDUCE, i)] = task 
			c.availableTasks <- task 
		}
	} else {
		log.Print("all reduce tasks are completed\n")
		close(c.availableTasks)
		c.stage = ""
	}
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
func (c *Coordinator) Done() bool {
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stage == ""
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage: MAP,
		nMap: len(files),
		nReduce: nReduce,
		tasks: map[string]Task{},
		availableTasks: make(chan Task, 
			int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	for i, file := range files {
		task := Task {
			Type: MAP,
			Index: i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}

	c.server()

	go func() {
		for {
			time.Sleep(time.Millisecond * 500)
			c.lock.Lock()

			for _, task := range c.tasks {
				// task.WorkerID != "" 说明当前的 task 已经被分配到一个 Worker 
				// 即任务已经发出但超时了还没有完成
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					// 重新放入任务池
					log.Printf("found timed out %s task %d on worker %s\n", 
								task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task 
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

func GenTaskID(tp string, index int) string {
	return fmt.Sprintf("%s-%d", tp, index)
}
