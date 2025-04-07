package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Command interface {}
	Term int 
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // 保护并发访问 Raft 共享状态
	peers     []*labrpc.ClientEnd // 所有 Raft 节点的 RPC 客户端
	persister *tester.Persister   // 用作持久化自己的状态，比如崩溃恢复时从磁盘读回
	me        int                 // 本节点的 id（在 peers[] 中的位置）
	dead      int32               // 是否已经被 Kill()，用来优雅退出

	// 持久化状态，必须保存到磁盘
	currentTerm int // 代表当前的任期，每经历一次任期都将 term++, leader 的心跳会自带 term
	votedFor int // 当前节点在当前任期内投给了哪个 ID 的节点，如果没有投则为 -1
	log []LogEntry // 日志条目，用户输入的指令

	// 易失状态，保存在内存
	commitIndex int // 当前整个 Raft 集群已经被“大多数节点确认”的日志的最大索引
	lastApplied int // 当前节点已经真正应用到本地状态机的最后一条日志索引

	// 只由 leader 维护的状态
	nextIndex []int // 对每个 follower，要发送的下一条日志的索引
	matchIndex []int // 对每个 follower，被确认复制成功的最高日志索引

	// 额外信息
	state int // 当前节点角色，0：follower 1: candidate 2: leader
	voteNum int // 当前 candidate 收到的投票数
	stamp time.Time // 最近一次收到 leader 心跳/请求的时间，用于判断心跳超时
	nextHeartBeat time.Time // 下一次应该发送心跳请求的时间

	// 与服务应用层交互
	applyChan chan raftapi.ApplyMsg // 用来通知应用层“可以应用一条日志了”（比如用户的 Put操作生效）

	// 快照，用于日志压缩
	snapshot []byte // 快照的二进制数据
	lastIncludedTerm int // 快照最后一条日志的任期 
	lastIncludedIndex int // 快照最后一条日志的索引
}

const (
	Follower = 0 
	Candidate = 1
	Leader = 2
	None = -1 
)
const (
	HeartBeatInterval = 100
	MaxElectionTime = 300 // 选举超时最大值，300ms
    MinElectionTime = 150 // 选举超时最小值，150ms
	ApplyInterval = 10 // 应用日志到状态机的检测频率
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

func (rf *Raft) ReadSnapshot(data []byte) {
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 创建一个 Raft 对象（new Raft）
// 初始化必要的字段
// 恢复崩溃前持久化的数据（日志、term、投票等）
// 启动后台线程（goroutines）做心跳检测、选举、应用日志
// 返回准备好的 *Raft 节点实例
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	length := len(rf.peers)
	rf.state = Follower
	rf.voteNum = 0
	rf.votedFor = None 
	rf.currentTerm = 0
	rf.matchIndex = make([]int, length) // 只会由 leader 维护
	rf.stamp = time.Now()

	rf.log = append(rf.log, LogEntry{
		Term: 0,
		Command: nil,
	})
	rf.applyChan = applyCh

	// 从持久化存储恢复状态
	rf.ReadSnapshot(persister.ReadSnapshot())	
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, length)

	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.applyToState() 

	return rf
}
