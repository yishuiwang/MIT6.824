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
	CommandValid bool
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
	ElectionTimer *time.Timer
	// TODO
	HeartbeatTimer *time.Timer

	// Persistent state on all servers
	State       state
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

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
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
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

	Debug(dInfo, "S%d receive vote request from S%d", rf.me, args.Id)
	Debug(dInfo, "S%d info: term: %d, votedFor: %d", rf.me, rf.CurrentTerm, rf.VotedFor)
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		Debug(dInfo, "S%d reject vote request from S%d", rf.me, args.Id)
		return
	}

	// If votedFor is null or candidateId, and candidate's logs are at
	// least as up-to-date as receiver's logs, grant vote (§5.2, §5.4)
	//if (rf.VotedFor == -1 || rf.VotedFor == args.Id) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
	//if rf.VotedFor == -1 || rf.VotedFor == args.Id {
	rf.VotedFor = args.Id
	rf.CurrentTerm = args.Term
	reply.VoteGranted = true
	Debug(dInfo, "S%d vote for S%d", rf.me, args.Id)
	return
	//}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 判断任期是否过期
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		Debug(dInfo, "S%d reject heartbeat from S%d", rf.me, args.LeaderId)
		return
	}

	// 如果是Leader发送的心跳
	if args.IsHeartbeat {
		// 检查自身状态
		Debug(dInfo, "S%d receive heartbeat from S%d", rf.me, args.LeaderId)
		switch rf.State {
		case Follower:
			rf.ElectionTimer.Reset(randTimeout(150, 300))
		case Candidate:
			rf.switchRole(Follower)
			rf.CurrentTerm = args.Term
			rf.ElectionTimer.Reset(randTimeout(150, 300))
		case Leader:
			rf.switchRole(Follower)
			rf.CurrentTerm = args.Term
			rf.ElectionTimer.Reset(randTimeout(150, 300))
		}
	}

	// Reply false if logs doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)

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
	rf.VotedFor = rf.me
	rf.ElectionTimer.Reset(randTimeout(150, 300))

	Debug(dInfo, "Term: %d, S%d: ElectLeader()", rf.CurrentTerm, rf.me)

	if rf.askForVote() {
		Debug(dLeader, "S%d become leader", rf.me)
		Debug(dLeader, "S%d: Term: %d, VotedFor: %d", rf.me, rf.CurrentTerm, rf.VotedFor)
		rf.switchRole(Leader)
		rf.heartbeat()
	} else {
		Debug(dInfo, "S%d stay candidate", rf.me)
	}
}

func (rf *Raft) askForVote() bool {
	var wg sync.WaitGroup
	voteCh := make(chan bool, len(rf.peers))
	result := 1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(index int) {
				args := &RequestVoteArgs{
					Term: rf.CurrentTerm,
					Id:   rf.me,
					// TODO
					LastLogIndex: rf.LastApplied,
					LastLogTerm:  rf.CurrentTerm,
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

func (rf *Raft) heartbeat() {
	// 向所有节点发送心跳
	for i := 0; i < len(rf.peers); i++ {
		args := &AppendEntriesArgs{
			Term:        rf.CurrentTerm,
			LeaderId:    rf.me,
			IsHeartbeat: true,
		}
		reply := &AppendEntriesReply{}
		if i != rf.me {
			Debug(dLeader, "S%d send heartbeat to S%d", rf.me, i)
			go rf.sendAppendEntries(i, args, reply)
		}
	}
	rf.HeartbeatTimer.Reset(HeartbeatInterval)
}

func (rf *Raft) switchRole(role state) {
	if rf.State == role {
		return
	}
	rf.State = role
	// TODO
	if role == Follower {
		rf.VotedFor = -1
	}
	if role == Candidate {
		rf.CurrentTerm++
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	rf.Log = make([]LogEntry, 0)
	rf.ElectionTimer = time.NewTimer(randTimeout(150, 300))
	rf.HeartbeatTimer = time.NewTimer(HeartbeatInterval)

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func randTimeout(min, max int) time.Duration {
	rand.NewSource(time.Now().UnixNano())
	return time.Duration(rand.Intn(max-min)+min) * time.Millisecond
}
