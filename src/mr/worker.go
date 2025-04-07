package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

type ByKey []KeyValue
func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
// Worker 不断向 Coordinator 的 ApplyForTask 发起 RPC 请求
// 这个函数会被多次调用来创建多个 Worker 进程
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := strconv.Itoa(os.Getpid())
	log.Printf("worker %s started\n", workerID)

	var prevTaskType string 
	var prevTaskIndex int 
	for {
		args := ApplyForTaskArgs {
			WorkerID: workerID,
			PrevTaskType: prevTaskType,
			PrevTaskIndex: prevTaskIndex,
		}
		reply := ApplyForTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)

		if reply.TaskType == "" {
			log.Print("received job completion signal\n")
			break
		}

		log.Printf("received %s task %d to worker %s\n", 
			reply.TaskType, reply.TaskIndex, workerID)
		
		if reply.TaskType == MAP {
			file, err := os.Open(reply.MapInputFile)
			if err != nil {
				log.Fatalf("failed to open %s: %e\n", reply.MapInputFile, err)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("failed to read %s: %e\n", reply.MapInputFile, err)
			}
			// 传入数据到 Map 函数，得到中间结果
			kvs := mapf(reply.MapInputFile, string(content))
			// 按 key 的哈希值对中间结果分桶
			hashkv := make(map[int][]KeyValue)
			for _, key_val := range kvs {
				hashed := ihash(key_val.Key) % reply.ReduceNum
				hashkv[hashed] = append(hashkv[hashed], key_val)
			}
			// 写入中间结果文件
			for i := 0; i < reply.ReduceNum; i++ {
				file, _ := os.Create(tempMapOutFile(workerID, 
						reply.TaskIndex, i))
				for _, kv := range hashkv[i] {
					fmt.Fprintf(file, "%v\t%v\n", kv.Key, kv.Value)
				}
				file.Close()
			}
		} else if reply.TaskType == REDUCE {
			var lines []string 
			for mapIndex := 0; mapIndex < reply.MapNum; mapIndex++ {
				inputFile := finalMapOutFile(mapIndex, reply.TaskIndex) 
				file, err := os.Open(inputFile)
				if err != nil {
					log.Printf("failed to open file %s: %e\n", inputFile, err)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Printf("failed to read file %s: %e\n", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			// 解析中间文件，获取键值对
			var kvs []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kvs = append(kvs, KeyValue{
					Key: parts[0],
					Value: parts[1],
				})
			}
			sort.Sort(ByKey(kvs))

			file, _ := os.Create(tempReduceOutFile(workerID, reply.TaskIndex))
			// 逐组处理相同 Key 的数据，调用 reducef
			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				var values []string 
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output_reduce := reducef(kvs[i].Key, values)
				fmt.Fprintf(file, "%v %v\n", kvs[i].Key, output_reduce)
				i = j
			}
			file.Close()
		}

		prevTaskType = reply.TaskType
		prevTaskIndex = reply.TaskIndex
		log.Printf("completed %s task %d\n", reply.TaskType, reply.TaskIndex)
	}	
	log.Printf("worker %s exits\n", workerID)
}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
