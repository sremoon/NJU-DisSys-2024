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
import "fmt"
import "math/rand"

// import "bytes"
// import "encoding/gob"

// Sugar for myself
type pair struct {
	First int
	Second int
}
func (A pair) Islarge(B pair) int {
	if A.First > B.First {
		return 1
	} else if A.First < B.First {
		return -1
	}
	// A.first == B.first
	if A.Second > B.Second {
		return 1
	} else if A.Second == B.Second {
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

const Leader = 0
const Candidate = 1
const Follower = 2

type Logs struct {
	Term int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//  Persistent state on all servers:
	CurrentTerm int
	VotedFor int
	Log []Logs 

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex []int
	MatchIndex []int

	// Additional args needed
	// Mark which states it is, leader / candidate / follower
	State int
	// get how many server voted
	NumVote int
	// time to do something, Stupid time.timer
	Timer *time.Timer
	ResetTimerFlag chan bool
}

// Some sugar function for Raft
func (rf *Raft) LastLogIndex() int { return len(rf.Log) - 1 }
func (rf *Raft) LastLogTerm() int { return rf.Log[rf.LastLogIndex()].Term }
const _Heartbeat = 0
const _BeCandidate = 1
func (rf *Raft) SetTimer(_type int) {
	fmt.Printf("Debug message: %d reset time\n", rf.me)
	// normal case is 5
	var coef int = 1
	if _type == _Heartbeat {
		var timePass int = 16 * coef
		rf.Timer = time.NewTimer(time.Duration(timePass) * time.Millisecond)
	} else {
		var timePass int = (60 + rand.Intn(30)) * coef
		rf.Timer = time.NewTimer(time.Duration(timePass) * time.Millisecond)
	}
}
func (rf *Raft) TermCheck(NowTerm int)  {
	if rf.CurrentTerm < NowTerm {
		rf.CurrentTerm = NowTerm
		rf.FollowerInit()
		go rf.FollowerRun()
		//fmt.Printf("Debug message: %d term changed to %d\n", rf.me, NowTerm)
		return 
	}
	return 
}
func (rf *Raft) FollowerInit() {
	rf.State = Follower
	rf.VotedFor = -1
}
func (rf *Raft) CandidateInit() {
	rf.State = Candidate
}
func (rf *Raft) LeaderInit() {
	rf.State = Leader

	rf.NextIndex = make([]int, 0)
	rf.MatchIndex = make([]int, 0)

	for i := 0; i < len(rf.peers); i ++ {
		rf.NextIndex = append(rf.NextIndex, len(rf.Log))
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	return rf.CurrentTerm, rf.State == Leader
}

/*
RequestVote
*/

type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
/*
	fmt.Printf("Debug message: requestvote %d[%d] from %d : args[(term)%d,(candidate)%d,%d,%d], reply[%d,%t]\n", 
		rf.me, rf.VotedFor,args.CandidateId,
		args.Term,args.CandidateId,args.LastLogIndex,args.LastLogTerm,
		reply.Term, reply.VoteGranted)
*/
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Case 0: too late 
	rf.TermCheck(args.Term)
	// Main part
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	// Case 1: too early term
	if args.Term < rf.CurrentTerm {
		return
	}
	// Case 2: Right term, get votes
	if	(rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && 
		(pair{args.LastLogTerm, args.LastLogIndex}.Islarge(pair{rf.LastLogTerm(), rf.LastLogIndex()}) != -1) {
			reply.VoteGranted = true
		rf.VotedFor = args.CandidateId	
	}
	if rf.State == Follower {
		rf.ResetTimerFlag <- true
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
	fmt.Printf("Debug message: %d[(term)%d] send requestvote %d : args[(term)%d,%d,%d,%d]\n", rf.me, rf.CurrentTerm,server,args.Term,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
	
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// I'm too late
	rf.TermCheck(reply.Term)
	if ok && rf.State == Candidate {
		if reply.VoteGranted == true && reply.Term == rf.CurrentTerm {
			rf.NumVote = rf.NumVote + 1
			if rf.NumVote > len(rf.peers) / 2 {
				rf.LeaderInit()
				go rf.LeaderRun()
			}
		}
	}

	//fmt.Printf("Debug message: %d send requestvote %d : reply[%d,%t]\n", rf.me, server,reply.Term, reply.VoteGranted)

	return ok
}

/*
AppendEntries
*/

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []Logs
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("Debug message: %d being calling : args[(term)%d,(leader)%d], nowterm[%d]\n", 
	rf.me, 
	args.Term,args.LeaderId,
	rf.CurrentTerm)

	// term check, self late
	rf.TermCheck(args.Term)
	// Case 1: Too late term
	reply.Term = rf.CurrentTerm
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		return
	}
	// Right term now
	if rf.State == Candidate {
		rf.FollowerInit()
		go rf.FollowerRun()
	}
	// Case 2: not right log
	if rf.State == Follower {
		rf.ResetTimerFlag <- true
	}
	if	(args.PreLogIndex < 0) || (args.PreLogIndex > rf.LastLogIndex()) || 
		(args.PreLogTerm != rf.Log[args.PreLogIndex].Term) {
		return
	}
	// Case 3: All right
	reply.Success = true
}

func (rf *Raft) angleBeatsOne(server int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok != true {
		return
	}
	rf.mu.Lock()
	rf.TermCheck(reply.Term)
	rf.mu.Unlock()
}

func (rf *Raft) angleBeatsAll() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		// skip myself
		if i == rf.me {
			continue
		}
		log_ := make([]Logs, 0)
		go rf.angleBeatsOne(i, AppendEntriesArgs{rf.CurrentTerm, rf.me, -1, -1, log_, rf.CommitIndex}, &AppendEntriesReply{})
	}
}

func (rf *Raft) FollowerRun() {

	fmt.Printf("Debug message: %d follower run\n", rf.me)
	for rf.State == Follower {
		rf.SetTimer(_BeCandidate)
		select {
			case flag := <-rf.ResetTimerFlag:
				if flag == true {
					continue
				}
			case <-rf.Timer.C:
				rf.CandidateInit()
				go rf.CandidateRun()
		}
	}
}

func (rf *Raft) CandidateRun() {
	for rf.State == Candidate {
		fmt.Printf("Debug message: %d candidate run\n", rf.me)
		rf.mu.Lock()
		// vote for self
		rf.CurrentTerm = rf.CurrentTerm + 1
		rf.VotedFor = rf.me
		rf.NumVote = 1
		rf.SetTimer(_BeCandidate)
		// ask for votes
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendRequestVote(i, RequestVoteArgs{rf.CurrentTerm, rf.me, rf.LastLogIndex(), rf.LastLogTerm()}, &RequestVoteReply{})
		}
		rf.mu.Unlock()
		if rf.State == Candidate {
			<-rf.Timer.C
		}
	}
}

func (rf *Raft) LeaderRun() {

	fmt.Printf("Debug message: %d leader run\n", rf.me)
	
	rf.SetTimer(_Heartbeat)
	rf.angleBeatsAll()
	if rf.State == Leader {	
		<- rf.Timer.C
	}
	// no logs now, so there is no difference
	for rf.State == Leader {
		rf.angleBeatsAll()
		rf.SetTimer(_Heartbeat)
		if rf.State == Leader {	
			<- rf.Timer.C
		}
	}	
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.CurrentTerm
	isLeader := rf.State == Leader

	if isLeader == false {
		index = -1
	} else {
		rf.Log = append(rf.Log, Logs{term})
		index = len(rf.Log) - 1
	}
	
	fmt.Printf("Debug message: start [%d] : %d, %d, %t\n", rf.me, index, term, isLeader)
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
	rf.CurrentTerm = 0
	rf.Log = make([]Logs, 0)
	rf.Log = append(rf.Log, Logs{-1})
	
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, 0)
	rf.MatchIndex = make([]int, 0)
	rf.Timer = time.NewTimer(0)
	rf.ResetTimerFlag = make(chan bool, 1)

	fmt.Printf("Debug message: %d init passed\n", rf.me)
	rf.FollowerInit()
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.FollowerRun()

	return rf
}
