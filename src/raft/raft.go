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

import "sync"
import "labrpc"
import "time"

// import "bytes"
// import "encoding/gob"

// Sugar for myself
type pair struct {
	first int
	second int
}
func (A pair) Islarge(B pair) int {
	if A.first > B.first {
		return 1
	} else if A.first < B.first {
		return -1
	}
	// A.first == B.first
	if A.second > B.second {
		return 1
	} else if A.second == B.second {
		return 0
	} else {
		return -1
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.

const leader = 0
const candidate = 1
const follower = 2

type Logs struct {
	term int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//  Persistent state on all servers:
	currentTerm int
	votedFor int
	log []Logs 

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex []int
	matchIndex []int

	// Additional args needed
	// Mark which states it is, leader / candidate / follower
	state int
	// get how many server voted
	numVote int
	// time to do something
	timer *time.Timer
}

// Some sugar function for Raft
func (rf *Raft) lastLogIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastLogTerm() int { return rf.log[rf.lastLogIndex()].term }
const _Heartbeat = 0
const _BeCandidate = 1
func (rf *Raft) setTimer(_type int) {
	if _type == _Heartbeat {
		rf.timer = time.NewTimer(time.Duration(90) * time.Millisecond)
	} else {
		// For debug, no random, hahaha
		rf.timer = time.NewTimer(time.Duration(300 + rf.me * 5) * time.Millisecond)
	}
}
func (rf *Raft) followerInit() {
	if rf.state == follower {
		return
	}
	rf.setTimer(_BeCandidate)
	rf.state = follower
	rf.votedFor = -1
}
func (rf *Raft) candidateInit() {
}
func (rf *Raft) leaderInit() {
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	return term, isleader
}

/*
Vote Part
*/

type RequestVoteArgs struct {
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}
type RequestVoteReply struct {
	term int
	voteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if  args.term > rf.currentTerm {
		rf.currentTerm = args.term
		rf.followerInit()
	}
	// Main part
	reply.term = rf.currentTerm
	reply.voteGranted = false
	if args.term < rf.currentTerm {
		return
	}
	if	(rf.votedFor == -1 || rf.votedFor == args.candidateId) && 
		(pair{args.lastLogTerm, args.lastLogIndex}.Islarge(pair{rf.lastLogTerm(), rf.lastLogIndex()}) != -1) {
		reply.voteGranted = true
		rf.votedFor = args.candidateId
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	
	if ok && rf.state == candidate {
		if rf.currentTerm < reply.term {
			rf.currentTerm = reply.term
			rf.followerInit()
		} else if reply.voteGranted == true && reply.term == rf.currentTerm {
			rf.numVote = rf.numVote + 1
			if rf.numVote > len(rf.peers) / 2 {
				rf.leaderInit()
			}
		}
	}

	return ok
}

/*
Normal Part
*/

type AppendEntriesArgs struct {
	term int
	leaderId int
	preLogIndex int
	preLogTerm int
	entries []Logs
	leaderCommit int
}

type AppendEntriesReply struct {
	term int
	success bool
}

func (rf *Raft) appendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.term > rf.currentTerm {
		rf.currentTerm = args.term
		rf.followerInit()
	}

	reply.term = rf.currentTerm
	reply.success = false
	if args.term < rf.currentTerm {
		return
	}
	// term is right now
	if rf.state == follower {
		rf.setTimer(_BeCandidate)
	}
	if	(args.preLogIndex < 0) || (args.preLogIndex > rf.lastLogIndex()) || 
		(args.preLogTerm != rf.log[args.preLogIndex].term) {
		return
	}

	reply.success = true
}

func (rf *Raft) angleBeats() {
	for i := range rf.peers {
		// skip myself
		if i == rf.me {
			continue
		}
		
	}
}

/*
General Part, e.g., no correlation to Raft main part
*/

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}


// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
