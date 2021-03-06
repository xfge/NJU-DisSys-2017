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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	StateLeader    = iota
	StateCandidate
	StateFollower
)

// HeartBeatInterval for interval between hearbeats
const HeartBeatInterval = 50 * time.Millisecond

// DebugMode for true shows logs
const DebugMode = false

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry contains command for state machine, and term when entry was received by leader (first index is 1)
type LogEntry struct {
	LogTerm int
	LogComd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         int
	voteCount     int
	chanCommit    chan bool
	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool

	// persistent state on all server
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries;

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on LEADER
	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term = rf.currentTerm
	return term, rf.state == StateLeader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// RequestVoteArgs RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogTerm  int
	LastLogIndex int
}

//
// RequestVoteReply RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// AppendEntriesArgs AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntriesReply AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		if DebugMode {
			fmt.Printf("Node %v (currentTerm: %v) rejects:%v term:%v", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		}
		return
	}

	// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	// Determine if the log is at least up to date
	isUpToDate := false
	if args.LastLogTerm > rf.getLastLogTerm() || args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex() {
		isUpToDate = true
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && isUpToDate {
		rf.chanGrantVote <- true
		rf.state = StateFollower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		if DebugMode {
			fmt.Printf("%v (currentTerm: %v) votes for:%v term:%v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		}
		return
	}

	if DebugMode {
		fmt.Printf("Node %v (currentTerm: %v) rejects:%v term:%v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	}
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastLogIndex() + 1
		if DebugMode {
			fmt.Printf("Node %v (currentTerm: %v) rejects:%v term:%v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		}
		return
	}

	rf.chanHeartbeat <- true

	if DebugMode {
		fmt.Printf("Node %v receives message from LeaderId:%v\n", rf.me, args.LeaderId)
	}

	// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}
	reply.Term = args.Term

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	// AssertTrue(args.Term == rf.currentTerm && args.PrevLogIndex <= rf.getLastLogIndex())
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	logTerm := rf.log[args.PrevLogIndex].LogTerm
	if args.PrevLogTerm != logTerm {
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].LogTerm != logTerm {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	// 4. Append any new entries not already in the log
	rf.log = rf.log[: args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)

	reply.Success = true
	reply.NextIndex = rf.getLastLogIndex() + 1

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getLastLogIndex() {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.currentTerm
		if rf.state != StateCandidate {
			return ok
		}
		if args.Term != term {
			return ok
		}
		// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == StateCandidate && rf.voteCount > len(rf.peers)/2 {
				rf.state = StateFollower
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != StateLeader {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}

		// If a leader discovers that its term is out of date, it immediately reverts to follower state.
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			rf.persist()
			if DebugMode {
				fmt.Printf("Node %v reverts to Follower State", rf.me)
			}
			return ok
		}
		if reply.Success {
			// not heart beat
			if len(args.Entries) > 0 {
				//fmt.Printf("Node %v reply true to leader %v: nextIndex:%v, appended log len:%v\n", server, rf.me, rf.nextIndex[server], len(args.Entries))
				//rf.nextIndex[server] += len(args.Entries)
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			//fmt.Printf("Node %v reply false to leader %v: nextIndex:%v len:%v\n", server, rf.me, reply.NextIndex, len(args.Entries))
			//rf.nextIndex[server] -= 1
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogTerm = rf.getLastLogTerm()
	args.LastLogIndex = rf.getLastLogIndex()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == StateCandidate {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentCommitIndex := rf.commitIndex
	lastLogIndex := rf.getLastLogIndex()
	for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {
		cnt := 1
		for j := range rf.peers {
			if rf.me != j && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm {
				cnt++
			}
		}
		if 2*cnt > len(rf.peers) {
			currentCommitIndex = i
		}
	}
	if currentCommitIndex != rf.commitIndex {
		rf.commitIndex = currentCommitIndex
		rf.chanCommit <- true
	}

	for i := range rf.peers {
		if rf.me != i && rf.state == StateLeader {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex >= len(rf.log) {
				fmt.Printf("Node %v: Something goes wrong. prevlogindex:%v, len(log):%v\n", rf.me, args.PrevLogIndex, len(rf.log))
			}
			args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
			args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
			copy(args.Entries, rf.log[args.PrevLogIndex+1:])
			args.LeaderCommit = rf.commitIndex
			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
			}(i, args)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == StateLeader
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogComd: command}) // append new entry from client
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = StateFollower
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case StateLeader:
				rf.broadcastAppendEntries()
				time.Sleep(HeartBeatInterval)
			case StateFollower:
				select {
				case <-rf.chanHeartbeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(rand.Int63()%233+100) * time.Millisecond):
					rf.state = StateCandidate
				}
			case StateCandidate:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.broadcastRequestVote()

				if DebugMode {
					fmt.Printf("Node %v ==> CANDIDATE for term %v\n", rf.me, rf.currentTerm)
				}

				select {
				case <-time.After(time.Duration(rand.Int63()%233+100) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.state = StateFollower
					if DebugMode {
						fmt.Printf("CANDIDATE Node %v receives chanHeartbeat\n", rf.me)
					}
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = StateLeader
					if DebugMode {
						fmt.Printf("CANDIDATE Node %v ==> LEADER\n", rf.me)
					}
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i].LogComd}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
