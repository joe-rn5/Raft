package raft

import (
	"lab2raft/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// RPC structs (all fields exported so they get encoded)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

// Make creates a Raft peer. If your starter code's Make has a different
// parameter list, keep the starter's signature and only copy the body.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:    peers,
		me:       me,
		applyCh:  applyCh,
		votedFor: -1,
		log:      []LogEntry{{Term: 0}}, // dummy entry at index 0
	}
	rf.cond = sync.NewCond(&rf.mu)
	rf.resetTimer()

	go rf.ticker()
	go rf.applier()
	return rf
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start appends a command to the leader's log and starts replicating it.
// Returns immediately: (index, term, isLeader).
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.broadcast()
	return index, rf.currentTerm, true
}

// Kill stops the background goroutines (harmless if the tests never call it).
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.cond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// ---------------------------------------------------------------------------
// Helpers (call with rf.mu held)
// ---------------------------------------------------------------------------

func (rf *Raft) resetTimer() {
	rf.lastHeard = time.Now()
	rf.timeout = time.Duration(electionMin+rand.Intn(electionRange)) * time.Millisecond
}

// Any message with a higher term makes us a follower of that term.
func (rf *Raft) becomeFollower(term int) {
	if rf.state == Leader {
		rf.resetTimer() // a leader's timer is stale; don't let it start an election instantly
	}
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.broadcast() // immediate heartbeat
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

// ticker drives both election timeouts (followers/candidates) and heartbeats (leader).
func (rf *Raft) ticker() {
	lastBeat := time.Now()
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			if time.Since(lastBeat) >= heartbeatInterval {
				lastBeat = time.Now()
				rf.broadcast()
			}
		} else if time.Since(rf.lastHeard) >= rf.timeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// applier delivers committed entries to applyCh in order, exactly once.
// The channel send happens WITHOUT the lock so it can never block RPC handlers.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
			continue
		}
		rf.lastApplied++
		msg := ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied].Command}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
	}
}

// ---------------------------------------------------------------------------
// Leader election
// ---------------------------------------------------------------------------

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetTimer()

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	votes := 1 // our own vote

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			if !rf.peers[i].Call("Raft.RequestVote", &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			// ignore stale replies
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			}
		}(i)
	}
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

// ---------------------------------------------------------------------------
// Log replication
// ---------------------------------------------------------------------------

// broadcast sends AppendEntries (entries or heartbeat) to every follower.
func (rf *Raft) broadcast() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicate(i)
		}
	}
}

// replicate sends one AppendEntries to peer i and processes the reply.
func (rf *Raft) replicate(i int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	prev := rf.nextIndex[i] - 1
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.log[prev].Term,
		Entries:      append([]LogEntry(nil), rf.log[prev+1:]...), // copy: sent outside the lock
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.peers[i].Call("Raft.AppendEntries", &args, &reply) {
		return // lost; the next heartbeat retries
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	// ignore stale replies
	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		// matchIndex only moves forward, so out-of-order replies are safe
		if match := prev + len(args.Entries); match > rf.matchIndex[i] {
			rf.matchIndex[i] = match
			rf.nextIndex[i] = match + 1
			rf.advanceCommit()
		}
		return
	}

	// Failure: skip if another reply already moved nextIndex for this peer.
	if rf.nextIndex[i] != prev+1 {
		return
	}

	// Log mismatch: jump back using the follower's hint, then retry right away.
	next := reply.ConflictIndex
	if next > len(rf.log) {
		next = len(rf.log)
	}
	if next < 1 {
		next = 1
	}
	rf.nextIndex[i] = next
	go rf.replicate(i)
}

// advanceCommit: commit the highest N replicated on a majority,
// but only if log[N] is from the current term (Raft's commit rule).
func (rf *Raft) advanceCommit() {
	for n := len(rf.log) - 1; n > rf.commitIndex && rf.log[n].Term == rf.currentTerm; n-- {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.cond.Signal()
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// Valid leader for this term: stay/become follower and reset the timer.
	rf.state = Follower
	rf.resetTimer()

	// Consistency check.
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log) // our log is too short
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// skip back to the first entry of the conflicting term
		idx := args.PrevLogIndex
		for idx > 1 && rf.log[idx-1].Term == rf.log[args.PrevLogIndex].Term {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	// Append new entries; truncate only on a real conflict (same index, different term).
	for k, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + k
		if idx < len(rf.log) {
			if rf.log[idx].Term == e.Term {
				continue
			}
			rf.log = rf.log[:idx]
		}
		rf.log = append(rf.log, args.Entries[k:]...)
		break
	}
	reply.Success = true

	// Advance commit index up to the last entry this RPC covered.
	if args.LeaderCommit > rf.commitIndex {
		last := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < last {
			last = args.LeaderCommit
		}
		if last > rf.commitIndex {
			rf.commitIndex = last
			rf.cond.Signal()
		}
	}
}