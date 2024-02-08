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
	"6.5840/labgob"
	"bytes"
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

	LastIncludeIndex int // 快照包含的最后的日志索引
	LastIncludeTerm  int
	LastLogIndex     int // len(log)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludeIndex)
	e.Encode(rf.LastIncludeTerm)
	e.Encode(rf.LastLogIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	var lastLogIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil ||
		d.Decode(&lastLogIndex) != nil {
		Debug(dError, "S%d read persist error", rf.me)
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.LastIncludeIndex = lastIncludeIndex
		rf.LastIncludeTerm = lastIncludeTerm
		rf.LastLogIndex = lastLogIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the logs through (and including)
// that index. Raft should now trim its logs as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastIncludeIndex {
		return
	}
	Debug(dSnap, "S%d log: %v", rf.me, rf.Log)
	Debug(dSnap, "S%d create snapshot, index: %d", rf.me, index)
	rf.LastIncludeTerm = rf.Log[index].Term
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
	data := rf.Log[index+1-rf.LastIncludeIndex:]
	rf.Log = append([]LogEntry{
		{Term: rf.LastIncludeTerm, Command: "init"},
	}, data...)
	rf.LastIncludeIndex = index
	Debug(dSnap, "S%d log: %v", rf.me, rf.Log)

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
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
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
		Debug(dVote, "S%d update term from %d to %d", rf.me, rf.CurrentTerm, args.Term)
		rf.CurrentTerm = args.Term
		rf.switchRole(Follower)
		// attention: 一定要重设VoteFor和ElectionTimer
		rf.VotedFor = args.Id
		rf.persist()
		rf.ElectionTimer.Reset(randTimeout())
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	update := false
	voterLastLog := rf.Log[rf.LastLogIndex-1]
	// 比较最后一个日志的任期号和索引
	if voterLastLog.Term < args.LastLogTerm {
		update = true
	}
	// 第一次选举时Term相同
	if voterLastLog.Term == args.LastLogTerm && rf.LastLogIndex-1 <= args.LastLogIndex {
		update = true
	}

	if (rf.VotedFor == -1 || rf.VotedFor == args.Id) && update {
		reply.VoteGranted = true
		rf.VotedFor = args.Id
		rf.persist()
		Debug(dVote, "S%d vote for S%d", rf.me, args.Id)
		return
	} else {
		reply.VoteGranted = false
		Debug(dVote, "S%d reject vote, voteFor %v, len log %v", rf.me, rf.VotedFor, rf.LastLogIndex)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1 Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		Debug(dWarn, "S%d current term: %d, args term: %d", rf.me, rf.CurrentTerm, args.Term)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		Debug(dTimer, "S%d reject heartbeat from S%d", rf.me, args.LeaderId)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.switchRole(Follower)
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
		rf.persist()
	}

	reply.Term = rf.CurrentTerm
	reply.Success = false

	// 回应心跳
	rf.ElectionTimer.Reset(randTimeout())
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d receive heartbeat from S%d,reset election", rf.me, args.LeaderId)
	}

	//args.PrevLogIndex = max(args.PrevLogIndex-rf.LastIncludeIndex, 0)
	//if args.PrevLogIndex < 0 {
	//	Debug(dError, "S%d prevLogIndex: %d, LastIncludeIndex: %d", rf.me, args.PrevLogIndex, rf.LastIncludeIndex)
	//	args.PrevLogIndex = 0
	//	//panic("prevLogIndex < 0")
	//}

	// 2 Reply false if logs don’t contain an entry at prevLogIndex
	if args.PrevLogIndex >= rf.LastLogIndex {
		// If a follower does not have prevLogIndex in its log,
		// it should return with conflictIndex = len(log) and conflictTerm = None.
		reply.ConflictIndex = rf.LastLogIndex
		reply.ConflictTerm = -1
		return
	}
	// TODO: 优化
	if rf.Log[args.PrevLogIndex-rf.LastIncludeIndex].Term != args.PrevLogTerm {
		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex].Term,
		// and then search its log for the first index whose entry has term equal to conflictTerm.
		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.Log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	// 3 If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it
	i, j := args.PrevLogIndex+1, 0
	// 删除这个冲突的日志项及其往后的日志
	Debug(dInfo, "S%d log: %v lastLogIndex: %d", rf.me, rf.Log, rf.LastLogIndex)
	for ; i < rf.LastLogIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.Log[i].Term != args.Entries[j].Term {
			// TODO
			//rf.LastLogIndex = i - 1 + rf.LastIncludeIndex
			// rf.LastLogIndex -=len(rf.Log[i:])
			rf.LastLogIndex = i
			rf.Log = rf.Log[:i]
			break
		}
	}
	// 4 Append any new entries not already in the logs
	Debug(dLog, "S%d receive log %v from S%d", rf.me, args.Entries, args.LeaderId)
	args.Entries = args.Entries[j:]
	Debug(dLog, "S%d append log %v", rf.me, args.Entries)
	rf.Log = append(rf.Log, args.Entries...)
	rf.LastLogIndex = len(args.Entries) + rf.LastLogIndex
	rf.persist()

	// 5 If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, rf.LastLogIndex-1+rf.LastIncludeIndex)
		Debug(dCommit, "S%d commitIndex: %d,leaderCommit: %d", rf.me, rf.CommitIndex, args.LeaderCommit)
		Debug(dLog, "S%d log: %v", rf.me, rf.Log)
	}

	reply.Success = true
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

func (rf *Raft) applyLogsLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		msg := make([]ApplyMsg, 0)
		for rf.CommitIndex > rf.LastApplied {
			rf.LastApplied++
			msg = append(msg, ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.LastApplied-rf.LastIncludeIndex].Command,
				CommandIndex: rf.LastApplied,
			})
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
				rf.persist()
				rf.mu.Unlock()
				rf.ElectLeader()
			} else {
				rf.mu.Unlock()
			}
		case <-rf.HeartbeatTimer.C:
			// 如果是Leader，发送心跳
			rf.mu.Lock()
			if rf.State == Leader {
				rf.mu.Unlock()
				rf.askForAppend(true)
			} else {
				rf.mu.Unlock()
			}
			continue
		}
	}
}

func (rf *Raft) ElectLeader() {
	rf.mu.Lock()
	rf.ElectionTimer.Reset(randTimeout())
	Debug(dVote, "Term: %d, S%d candidate begin elect ", rf.CurrentTerm, rf.me)
	rf.mu.Unlock()

	// TODO 成为leader后，还会触发 stay candidate
	if rf.askForVote() {
		rf.mu.Lock()
		Debug(dLeader, "Term: %d, S%d become leader", rf.CurrentTerm, rf.me)
		rf.switchRole(Leader)
		rf.mu.Unlock()
		// 当选后立即发送心跳
		rf.askForAppend(true)
	} else {
		Debug(dVote, "S%d stay candidate", rf.me)
	}
}

func (rf *Raft) askForVote() bool {
	voteCh := make(chan bool, len(rf.peers))
	result := 1
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		Id:           rf.me,
		LastLogIndex: rf.LastLogIndex - 1,
		LastLogTerm:  rf.Log[rf.LastLogIndex-1].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendVote(i, args, voteCh)
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

func (rf *Raft) sendVote(index int, args *RequestVoteArgs, voteCh chan bool) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(index, args, reply)
	if !ok {
		//Debug(dVote, "S%d send vote to S%d timeout", rf.me, index)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 处理RPC回复之前先判断
	if rf.State != Candidate || rf.CurrentTerm != args.Term {
		//Debug(dError, "S%d is not candidate, cant retry", rf.me)
		return
	}
	Debug(dVote, "S%d receive vote reply from S%d,res: %v", rf.me, index, reply.VoteGranted)
	if rf.CurrentTerm < reply.Term {
		// 跟进任期，等待选举
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.switchRole(Follower)
		rf.persist()
		return
	}
	if reply.VoteGranted {
		voteCh <- true
		return
	}
}

func (rf *Raft) askForAppend(heartbeat bool) {
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
		if rf.LastLogIndex >= rf.NextIndex[i] || heartbeat {
			nextIndex := max(rf.NextIndex[i]-rf.LastIncludeIndex, 1)
			entries := rf.Log[nextIndex:]
			args := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.CommitIndex,
				PrevLogIndex: rf.NextIndex[i] - 1,
				PrevLogTerm:  rf.Log[nextIndex-1].Term,
				Entries:      entries,
			}
			if heartbeat {
				Debug(dTimer, "S%d send heartbeat to S%d", rf.me, i)
			} else {
				Debug(dLog, "S%d send log %v to S%d", rf.me, entries, i)
			}

			// 并发地向所有Follower节点复制数据并等待接收响应ACK
			go rf.sendLog(i, args)
		}
	}
}

func (rf *Raft) sendLog(index int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(index, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 处理RPC回复之前先判断
	if rf.State != Leader || rf.CurrentTerm != args.Term {
		Debug(dError, "S%d is not leader, cant retry", rf.me)
		return
	}
	//Debug(dLog, "S%d receive reply from S%d,res: %v", rf.me, index, reply.Success)
	if rf.CurrentTerm < reply.Term {
		// 跟进任期，等待选举
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.switchRole(Follower)
		rf.persist()
		return
	}

	if reply.Success {
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		// 确保matchIndex单调递增
		rf.MatchIndex[index] = max(rf.MatchIndex[index], args.PrevLogIndex+len(args.Entries))
		rf.NextIndex[index] = rf.MatchIndex[index] + 1
		// if there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).
		for i := rf.LastLogIndex - 1; i > rf.CommitIndex; i-- {
			count := 0
			for j := 0; j < len(rf.peers); j++ {
				if rf.MatchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.Log[i].Term == rf.CurrentTerm {
				rf.CommitIndex = i
				// leader提交log，将自己commitIndex发送给所有follower
				Debug(dCommit, "S%d current commit: %d,log len:%d", rf.me, rf.CommitIndex, rf.LastLogIndex)
				break
			}
		}
		return
	} else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)

		// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
		// If it finds an entry in its log with that term,
		// it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
		if reply.ConflictTerm != -1 {
			found := false
			for i := rf.LastLogIndex - 1; i > 0; i-- {
				if rf.Log[i].Term == reply.ConflictTerm {
					rf.NextIndex[index] = i + 1
					found = true
					break
				}
			}
			if !found {
				rf.NextIndex[index] = reply.ConflictIndex
			}
		} else {
			// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
			rf.NextIndex[index] = reply.ConflictIndex
		}
		nextIndex := max(1, rf.NextIndex[index])
		newArgs := &AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.CommitIndex,
			PrevLogIndex: rf.NextIndex[index] - 1,
			PrevLogTerm:  rf.Log[nextIndex-1].Term,
			Entries:      rf.Log[nextIndex:],
		}
		Debug(dLog2, "S%d retry append entries to S%d", rf.me, index)
		go rf.sendLog(index, newArgs)
	}
	return
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
	if role == Follower {
		rf.VotedFor = -1
		Debug(dVote, "S%d become follower", rf.me)
	}
	if role == Leader {
		// 更新nextIndex[]和matchIndex[]
		for i := 0; i < len(rf.peers); i++ {
			rf.NextIndex[i] = rf.LastLogIndex
			rf.MatchIndex[i] = 0
		}
		Debug(dLeader, "S%d nextIndex: %v, matchIndex: %v", rf.me, rf.NextIndex, rf.MatchIndex)
	}
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
	rf.Log = append(rf.Log, LogEntry{Command: command, Term: rf.CurrentTerm})
	rf.LastLogIndex++
	index := rf.LastLogIndex - 1
	term := rf.CurrentTerm
	rf.persist()
	rf.NextIndex[rf.me] = rf.LastLogIndex
	rf.MatchIndex[rf.me] = rf.LastLogIndex - 1
	Debug(dLog, "S%d add log %d,%v", rf.me, index, command)
	Debug(dInfo, "S%d lastLogIndex: %d", rf.me, rf.LastLogIndex)
	rf.mu.Unlock()

	go rf.askForAppend(false)
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
	Debug(dInfo, "S%d: Kill()", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	Debug(dLog2, "S%d log: %v lastlogIndex: %d", rf.me, rf.Log, rf.LastLogIndex)
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
	rf.ElectionTimer = time.NewTimer(randTimeout())
	//rf.ElectionTimer.Reset(randTimeout(250, 400))
	rf.HeartbeatTimer = time.NewTimer(HeartbeatInterval)

	rf.LastIncludeIndex = 0
	rf.LastIncludeTerm = 0
	rf.LastLogIndex = 1

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
	data := persister.ReadRaftState()
	rf.readPersist(data)
	rf.persist()

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applyCh goroutine to apply logs
	go rf.applyLogsLoop(applyCh)

	return rf
}

func randTimeout() time.Duration {
	rand.NewSource(time.Now().UnixNano())
	low := 150
	high := 300
	return time.Duration(rand.Intn(high-low)+low) * time.Millisecond
}
