package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool // 当ApplyMsg用于apply指令时为true
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const HeartbeatInterval = 100 * time.Millisecond

type state int

const (
	Follower state = iota
	Candidate
	Leader
)

type LogEntry struct {
	// Log entries
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	ElectionTimer  *time.Timer
	HeartbeatTimer *time.Timer

	// Persistent state on all servers
	State       state
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	apply       chan ApplyMsg
	// Volatile state on all servers
	CommitIndex int
	LastApplied int // 用于记录已经被应用到状态机的最后一条日志索引

	// Volatile state on leaders
	NextIndex  []int
	MatchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == Leader
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the logs through (and including)
// that index. Raft should now trim its logs as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	Id           int // candidate's Id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //前一条日志索引
	PrevLogTerm  int //前一条日志任期
	Entries      []LogEntry
	LeaderCommit int //领导者已提交的日志索引
	IsHeartbeat  bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 每一轮任期中，只会出现一位领导者，或者没有领导者。
	// 投票者必须确保candidate的日志至少与自己的一样新或更新才会给它投票
	// 这是为了保证当选的leader一定包含之前term所有已提交的日志

	Debug(dVote, "S%d receive vote request from S%d", rf.me, args.Id)
	// 收到任期比自身小的请求直接丢弃
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		Debug(dVote, "Term is lower, S%d reject vote request from S%d", rf.me, args.Id)
		return
	}

	// 收到任期比自身任期大的请求时，需要马上跟随对方并更新自己的任期
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.Id
		rf.switchRole(Follower)
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	update := false
	voterLastLog := rf.Log[len(rf.Log)-1]
	// 比较最后一个日志的任期号和索引
	if voterLastLog.Term < args.LastLogTerm {
		update = true
	}
	// 第一次选举时Term相同
	if voterLastLog.Term == args.LastLogTerm && len(rf.Log)-1 <= args.LastLogIndex {
		update = true
	}

	if (rf.VotedFor == -1 || rf.VotedFor == args.Id) && update {
		reply.VoteGranted = true
		rf.VotedFor = args.Id
		Debug(dVote, "S%d vote for S%d", rf.me, args.Id)
		return
	} else {
		reply.VoteGranted = false
		Debug(dVote, "S%d already vote for S%d", rf.me, rf.VotedFor)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1 Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		Debug(dInfo, "S%d reject heartbeat from S%d", rf.me, args.LeaderId)
		return
	}

	reply.Term = rf.CurrentTerm
	reply.Success = true

	// 如果是Leader发送的心跳
	if args.IsHeartbeat {
		rf.ElectionTimer.Reset(randTimeout(150, 300))
		if len(args.Entries) == 0 {
			Debug(dTimer, "S%d receive heartbeat from S%d,reset election", rf.me, args.LeaderId)
		}
		rf.switchRole(Follower)
		rf.CurrentTerm = args.Term
	}

	// 如果是Leader发送的日志
	Debug(dLog, "S%d receive logs from S%d", rf.me, args.LeaderId)
	// 2 Reply false if logs don’t contain an entry at prevLogIndex
	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		Debug(dLog2, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		return
	}

	// 3 If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	// TODO
	//i := 0
	//for index := args.PrevLogIndex + 1; index < len(rf.Log); index++ {
	//	if rf.Log[index].Term != args.Entries[i].Term {
	//		rf.Log = rf.Log[:index]
	//		break
	//	}
	//	i++
	//}

	// 4 Append any new entries not already in the logs
	rf.Log = append(rf.Log, args.Entries...)

	Debug(dLog2, "S%d Append entries success.", rf.me)

	// 5 If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
		Debug(dCommit, "S%d commitIndex: %d,leaderCommit: %d", rf.me, rf.CommitIndex, args.LeaderCommit)
	}

	return
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// TODO 定期将已提交日志同步
func (rf *Raft) applyLogsLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		msg := make([]ApplyMsg, 0)
		for rf.CommitIndex > rf.LastApplied {
			Debug(dCommit, "S%d log len: %d, commitIndex: %d, lastApplied: %d", rf.me, len(rf.Log), rf.CommitIndex, rf.LastApplied)
			rf.LastApplied++
			msg = append(msg, ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.LastApplied].Command,
				CommandIndex: rf.LastApplied,
			})
			Debug(dCommit, "S%d apply log %v", rf.me, rf.LastApplied)
		}
		rf.mu.Unlock()
		for _, v := range msg {
			applyCh <- v
		}
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		select {
		case <-rf.ElectionTimer.C:
			// 如果选举超时，发起选举
			rf.mu.Lock()
			if rf.State == Follower || rf.State == Candidate {
				rf.switchRole(Candidate)
				rf.mu.Unlock()
				rf.ElectLeader()
			} else {
				rf.mu.Unlock()
			}
		case <-rf.HeartbeatTimer.C:
			// 如果是Leader，发送心跳
			rf.mu.Lock()
			if rf.State == Leader {
				rf.heartbeat()
			}
			rf.mu.Unlock()
			continue
		}
	}
}

func (rf *Raft) ElectLeader() {
	rf.mu.Lock()
	rf.ElectionTimer.Reset(randTimeout(150, 300))
	Debug(dVote, "Term: %d, S%d begin elect leader", rf.CurrentTerm, rf.me)
	rf.mu.Unlock()

	if rf.askForVote() {
		rf.mu.Lock()
		Debug(dLeader, "Term: %d, S%d become leader", rf.CurrentTerm, rf.me)
		rf.switchRole(Leader)
		rf.heartbeat()
		rf.mu.Unlock()
	} else {
		Debug(dInfo, "S%d stay candidate", rf.me)
	}
}

func (rf *Raft) askForVote() bool {
	voteCh := make(chan bool, len(rf.peers))
	result := 1
	rf.mu.Lock()
	term := rf.CurrentTerm
	LogIndex := rf.LastApplied
	LogTerm := rf.Log[len(rf.Log)-1].Term
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int) {
				args := &RequestVoteArgs{
					Term:         term,
					Id:           rf.me,
					LastLogIndex: LogIndex,
					LastLogTerm:  LogTerm,
				}
				reply := &RequestVoteReply{}
				Debug(dInfo, "S%d send vote request to S%d", rf.me, index)
				if rf.sendRequestVote(index, args, reply) {
					if reply.VoteGranted {
						voteCh <- true
					}
				}
			}(i)
		}
	}

	for {
		select {
		case <-voteCh:
			result++
			if result > len(rf.peers)/2 {
				return true
			}
		case <-time.After(50 * time.Millisecond):
			return false
		}
	}
}

func (rf *Raft) askForAppend() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 重置心跳定时器
	rf.HeartbeatTimer.Reset(HeartbeatInterval)

	// 发送备份请求
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// If last log index ≥ nextIndex for a follower:
		// send AppendEntries RPC with log entries starting at nextIndex
		if len(rf.Log) >= rf.NextIndex[i] {
			// 从nextIndex开始复制日志
			nextIndex := rf.NextIndex[i]
			entries := rf.Log[nextIndex:]
			args := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.CommitIndex,
				// nextIndex - 1是前一条日志的索引
				PrevLogIndex: rf.NextIndex[i] - 1,
				PrevLogTerm:  rf.Log[nextIndex-1].Term,
				Entries:      entries,
				IsHeartbeat:  true,
			}
			// 并发地向所有Follower节点复制数据并等待接收响应ACK
			Debug(dLog, "S%d send entries to S%d,entries len: %d", rf.me, i, len(entries))
			go rf.sendLog(i, args)
		}
		// TODO default
	}
}

func (rf *Raft) sendLog(index int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	rf.sendAppendEntries(index, args, reply)
	Debug(dLog, "S%d receive append entries reply from S%d", rf.me, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		rf.NextIndex[index] = len(rf.Log)
		rf.MatchIndex[index] = len(rf.Log) - 1
		// if there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).
		for i := len(rf.Log) - 1; i > rf.CommitIndex; i-- {
			count := 1
			for j := 0; j < len(rf.peers); j++ {
				if rf.MatchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.Log[i].Term == rf.CurrentTerm {
				rf.CommitIndex = i
				Debug(dCommit, "S%d commit log %v", rf.me, i)
				// leader提交log，将自己commitIndex发送给所有follower
				//go rf.askForAppend()
				break
			}
		}
		return
	} else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		rf.NextIndex[index]--
		nextIndex := rf.NextIndex[index]
		entries := rf.Log[nextIndex:]
		// TODO 精简
		newArgs := &AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.CommitIndex,
			PrevLogIndex: rf.NextIndex[index] - 1,
			PrevLogTerm:  rf.Log[nextIndex-1].Term,
			Entries:      entries,
			IsHeartbeat:  true,
		}
		Debug(dLog2, "S%d retry append entries to S%d", rf.me, index)
		go rf.sendLog(index, newArgs)
	}
	return
}

func (rf *Raft) heartbeat() {
	// 向所有节点发送心跳
	for i := 0; i < len(rf.peers); i++ {
		args := &AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.CommitIndex,
			// TODO
			PrevLogIndex: rf.LastApplied,
			PrevLogTerm:  rf.Log[rf.LastApplied].Term,
			IsHeartbeat:  true,
		}
		reply := &AppendEntriesReply{}
		if i != rf.me {
			Debug(dTimer, "S%d send heartbeat to S%d", rf.me, i)
			go rf.sendAppendEntries(i, args, reply)
		}
	}
	rf.HeartbeatTimer.Reset(HeartbeatInterval)
}

func (rf *Raft) switchRole(role state) {
	if role == Candidate {
		rf.CurrentTerm++
		rf.VotedFor = rf.me
	}
	if rf.State == role {
		return
	}
	rf.State = role
	// TODO
	//Debug(dInfo, "S%d switch to %d", rf.me, role)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.State != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	index := len(rf.Log)
	term := rf.CurrentTerm
	rf.Log = append(rf.Log, LogEntry{Command: command, Term: rf.CurrentTerm})
	rf.NextIndex[rf.me] = len(rf.Log)
	rf.MatchIndex[rf.me] = len(rf.Log) - 1
	Debug(dLog, "S%d add log %d", rf.me, index)
	rf.mu.Unlock()

	go rf.askForAppend()
	// return immediately.
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing Debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	Debug(dInfo, "S%d: Kill()", rf.me)
	Debug(dLog, "S%d: log len: %d commitIndex: %d lastApplied: %d", rf.me, len(rf.Log), rf.CommitIndex, rf.LastApplied)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	Debug(dInfo, "S%d: Make()", rf.me)
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.State = Follower
	rf.ElectionTimer = time.NewTimer(randTimeout(150, 300))
	rf.HeartbeatTimer = time.NewTimer(HeartbeatInterval)

	rf.CommitIndex = 0
	rf.LastApplied = 0
	// 合法日志索引从1开始
	rf.Log = []LogEntry{{Term: -1, Command: "init"}}
	rf.apply = applyCh
	rf.NextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.NextIndex[i] = 1
	}
	rf.MatchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applyCh goroutine to apply logs
	// TODO
	go rf.applyLogsLoop(applyCh)

	return rf
}

func randTimeout(min, max int) time.Duration {
	rand.NewSource(time.Now().UnixNano())
	return time.Duration(rand.Intn(max-min)+min) * time.Millisecond
}
