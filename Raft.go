package raft

import (
	"labrpc"
	"sync"
	"time"
)
import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
// ApplyMsg is sent on applyCh for every committed entry.
// If your starter code already defines ApplyMsg, keep ITS version and delete this one.
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignored in this lab
	Snapshot    []byte // ignored in this lab
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	heartbeatInterval = 110 * time.Millisecond // < 10 heartbeats/sec
	electionMin       = 300                    // ms
	electionRange     = 200                    // ms -> timeout in [300, 500)
)

type Raft struct {
	mu      sync.Mutex
	peers   []*labrpc.ClientEnd
	me      int
	applyCh chan ApplyMsg
	cond    *sync.Cond // wakes the applier when commitIndex moves
	dead    int32

	// Raft state (log[0] is a dummy entry, so real entries start at index 1)
	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	// leader only
	nextIndex  []int
	matchIndex []int

	// election timer
	lastHeard time.Time
	timeout   time.Duration
}

// RPC structs (all fields exported so they get encoded)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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
	Term          int
	Success       bool
	ConflictIndex int // hint: where the leader should retry from
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Kill stops the background goroutines (harmless if the tests never call it).
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.cond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// Helpers (call with rf.mu held)

func (rf *Raft) resetTimer() {
	rf.lastHeard = time.Now()
	rf.timeout = time.Duration(electionMin+rand.Intn(electionRange)) * time.Millisecond
}

// Any message with a higher term makes us a follower of that term.
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	// Election restriction: candidate's log must be at least as up-to-date as ours.
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetTimer()
	}
}