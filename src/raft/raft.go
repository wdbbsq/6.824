package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"log"
	"math/rand"
	"time"
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// use *type alias* instead of custome type
type State = int32

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	HeartbeatInterval = 100 * time.Millisecond
	RequestTimout
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

type LogEntry struct {
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
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	timer  *time.Timer
	state  State
	random *rand.Rand
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, atomic.LoadInt32(&rf.state) == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		atomic.StoreInt32(&rf.state, Follower)
	}
	// vote for nobody or candidate's log is up-to-date
	if rf.votedFor == -1 {
		//(args.LastLogTerm == rf.currentTerm && args.LastLogIndex == rf.commitIndex) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries send heartbeats periodically by leader
// when logEntry is empty
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// receive a heartbeat
	if args.Entries == nil || len(args.Entries) == 0 {
		rf.resetTimer()
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			atomic.StoreInt32(&rf.state, Follower)
		}
		log.Printf("%v-%v get heartbeat from %v-%v\n", rf.me, rf.currentTerm,
			args.LeaderId, args.Term)

		//switch atomic.LoadInt32(&rf.state) {
		//case Candidate:
		//	// we lost qaq
		//	if args.Term >= rf.currentTerm {
		//		atomic.StoreInt32(&rf.state, Follower)
		//		rf.currentTerm = args.Term
		//	}
		//case Leader:
		//	if args.Term > rf.currentTerm {
		//		rf.currentTerm = args.Term
		//	}
		//	//rf.currentTerm = args.Term
		//default:
		//}

		return
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply,
	group *sync.WaitGroup, voteCh chan bool) {

	defer group.Done()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimout)
	defer cancel()

	select {
	case voteCh <- rf.peers[server].Call("Raft.RequestVote", args, reply):
	case <-ctx.Done():
		log.Printf("RequestVote for %v timeout\n", rf.me)
	}
}

func (rf *Raft) sendRpcRequest(server int, method string, args interface{}, reply interface{},
	group *sync.WaitGroup, resultCh chan interface{}) {

	defer group.Done()

	ok := rf.peers[server].Call(method, args, reply)
	if ok {
		resultCh <- reply
	}

	//ctx, cancel := context.WithTimeout(context.Background(), RequestTimout)
	//defer cancel()
	//okCh := make(chan bool)
	//defer close(okCh)
	//go func() {
	//	okCh <- rf.peers[server].Call(method, args, reply)
	//}()
	//
	//select {
	//case ok := <-okCh:
	//	if ok {
	//		resultCh <- reply
	//	}
	//case <-ctx.Done():
	//	DPrintf("RequestVote for %v timeout\n", rf.me)
	//}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// todo handle timeout
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			// start an election
			rf.mu.Lock()
			rf.resetTimer()
			rf.currentTerm++
			// vote for myself first
			rf.votedFor = rf.me
			// release lock first
			rf.mu.Unlock()

			atomic.StoreInt32(&rf.state, Candidate)

			DPrintf("Server %v-%v started an election\n", rf.me, rf.currentTerm)

			voteCh := make(chan interface{}, len(rf.peers))
			group := &sync.WaitGroup{}

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				group.Add(1)
				go rf.sendRpcRequest(i, "Raft.RequestVote", &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}, &RequestVoteReply{}, group, voteCh)
			}
			group.Wait()

			votes := 0
			for reply := range voteCh {
				if reply.(*RequestVoteReply).VoteGranted {
					votes++
				}
			}
			// leader now!
			if votes > len(rf.peers)/2 {
				atomic.StoreInt32(&rf.state, Leader)
				DPrintf("Leader: %v-%v\n", rf.me, rf.currentTerm)
			}
			close(voteCh)
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		for atomic.LoadInt32(&rf.state) == Leader {
			DPrintf("%v-%v sending heartbeat\n", rf.me, rf.currentTerm)
			for i := 0; i < len(rf.peers); i++ {
				reply := &AppendEntriesReply{}
				if !rf.sendAppendEntries(i, &AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
					Entries:  nil,
				}, reply) {
					log.Println("Err when sending AppendEntries")
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					atomic.StoreInt32(&rf.state, Follower)
					break
				}
			}
			time.Sleep(HeartbeatInterval)
		}
	}
}

func (rf *Raft) resetTimer() {
	// raft paper suggests 150-300 ms
	// 6.824: Such a range only makes sense if the leader sends
	// heartbeats considerably more often than once per 150 milliseconds.
	// So a larger range is suggested (300-
	rf.timer.Stop()
	timeout := rf.random.Intn(150) + 300
	rf.timer.Reset(time.Duration(timeout) * time.Millisecond)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	atomic.StoreInt32(&rf.state, Follower)
	rf.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.timer = time.NewTimer(time.Minute)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	rf.resetTimer()

	return rf
}
