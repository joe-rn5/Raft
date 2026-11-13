package raft

import (
	"labrpc"
	"sync"
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