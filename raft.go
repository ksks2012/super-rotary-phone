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
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"github.com/ksks2012/super-rotary-phone/labrpc"
)

var TIKER_SLEEP_SEC int64 = 300      //ms
var HEATHBEAT_SLEEP_SEC int64 = 200  //ms
var LOG_COMMIT_SLEEP_SEC int64 = 100 //ms
var RPC_TIMEOUT_SEC int64 = 50       //ms

type RaftRole int

const (
	Follower RaftRole = iota
	Candidate
	Leader
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

type Entry struct {
	Term  int
	Index int
	Value interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// timer
	electionTimer  *time.Ticker
	heartbeatTimer *time.Ticker

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        RaftRole
	currentTerm int
	votedFor    int
	logEntries  []Entry

	// Known to be committed
	commitIndex int
	// Applied to state machine
	lastApplied int

	// TODO: For leader
	// matchIndex -> resend?
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == Leader)
	fmt.Printf("[GetState] Raft: %v | Term: %v | Role: %v | Vote for %v\n", rf.me, rf.currentTerm, rf.role, rf.votedFor)
	return term, isleader
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

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // Last commit index (?)
	PrevLogTerm  int // Term in PrevLogIndex
	Entries      []Entry
	LeaderCommit int
}

func (req *AppendEntriesRequest) print() {
	fmt.Printf("[Req Info] Term: %v | LeaderId: %v | PrevLogIndex: %v | PrevLogTerm: %v | Entries: %v | LeaderCommit: %v\n", req.Term, req.LeaderId, req.PrevLogIndex, req.PrevLogTerm, len(req.Entries), req.LeaderCommit)
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	LogState      int // State to append log entries
	CurrentCommit int // Current commit of follower
	CommitTerm    int // Term of current commit
}

func (rf *Raft) RequestHeartbeat(req *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	if req.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.role = Follower
	rf.currentTerm = req.Term
	rf.votedFor = req.LeaderId

	// Reset election timer
	sleepSeconds := TIKER_SLEEP_SEC + (rand.Int63() % (TIKER_SLEEP_SEC))
	rf.electionTimer.Reset(time.Duration(sleepSeconds) * time.Millisecond)

	fmt.Printf("[RequestHeartbeat] Raft: %v | Term: %v | Role: %v | Commit Index: %v\n", rf.me, rf.currentTerm, rf.role, rf.commitIndex)
	req.print()
	if req.PrevLogIndex != rf.commitIndex {
		reply.CurrentCommit = rf.commitIndex
		reply.CommitTerm = 0
		if len(rf.logEntries) > 0 && reply.CurrentCommit > 0 {
			reply.CommitTerm = rf.logEntries[reply.CurrentCommit-1].Term
		}
	}
	// log entries from leader had updated
	if req.LeaderCommit > rf.commitIndex {
		// TODO: If leaderCommit > commitIndex => set commitIndex = min(leaderCommit, index of last new entry)
		// rf.commitIndex = Min(req.LeaderCommit, rf.commitIndex)
		// TODO: multiple append?
		// TODO: Proccess conflict logs
		fmt.Printf("[RequestHeartbeat] Raft: %v | Term: %v | Role: %v | Before rf.logEntries: %v\n", rf.me, rf.currentTerm, rf.role, len(rf.logEntries))
		// Append all of log entry
		for i := 0; i < len(req.Entries); i++ {
			rf.logEntries = append(rf.logEntries, req.Entries[i])
			rf.commitIndex++
		}
		fmt.Printf("[RequestHeartbeat] Raft: %v | Term: %v | Role: %v | After rf.logEntries: %v\n", rf.me, rf.currentTerm, rf.role, len(rf.logEntries))
		reply.CurrentCommit = rf.commitIndex
		// TODO: Check log prevLogIndex, prevLogTerm
		// TODO: Check log is not conflict
	} else {
		// TODO: revert commit
		// TODO: set reply
		// NOTE: Leader will re-send log entries
		reply.CurrentCommit = rf.commitIndex
		// reply.CommitTerm = rf.logEntries[reply.CurrentCommit].Term
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[RequestVote] Raft: %v | Term: %v | Role: %v | Votereq from %v(%v)\n", rf.me, rf.currentTerm, rf.role, args.CandidateId, args.Term)
	reply.Term = 0
	reply.VoteGranted = false

	if args.Term > rf.currentTerm && rf.role != Candidate {
		reply.Term = args.Term
		reply.VoteGranted = true

		rf.role = Follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
	}
	fmt.Printf("[RequestVote] Raft: %v | Term: %v | Role: %v | Vote %v\n", rf.me, rf.currentTerm, rf.role, rf.votedFor)
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

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartbeat", args, reply)
	return ok
}

func (rf *Raft) sendEntries2Peers(resend bool) bool {
	lengthPeers := len(rf.peers)
	replyChannel := make(chan int, lengthPeers-1)

	var wg sync.WaitGroup
	wg.Add(lengthPeers)
	for i := 0; i < lengthPeers; i++ {
		go func(i int) {
			defer wg.Done()
			if i != rf.me {
				appendEntriesRequest := rf.genEntriesReq(i, rf.commitIndex)
				appendEntriesReply := AppendEntriesReply{}
				c := make(chan bool, 1)
				go func() {
					c <- rf.sendHeartbeat(i, &appendEntriesRequest, &appendEntriesReply)
				}()
				select {
				case err := <-c:
					// use err and result
					fmt.Printf("[sendEntries2Peers %v] %v\n", rf.me, err)
					if appendEntriesReply.Success {
						replyChannel <- 1
					} else {
						replyChannel <- 0
					}
					if appendEntriesReply.Term > rf.currentTerm {
						rf.role = Follower
						rf.votedFor = -1
					}
					fmt.Printf("[sendEntries2Peers] Raft: %v | Term: %v | Role: %v | Resend entries to %v: reply->%v cur->%v applied->%v\n", rf.me, rf.currentTerm, rf.role, i, appendEntriesReply.CurrentCommit, rf.commitIndex, rf.lastApplied)
					// TODO: resend
					// if appendEntriesReply.CurrentCommit < rf.lastApplied && resend {
					// 	resendEntries := rf.genEntriesReq(appendEntriesReply.CurrentCommit)
					// 	rf.sendEntries2Peers(&resendEntries, false)
					// }
				case <-time.After(time.Duration(RPC_TIMEOUT_SEC) * time.Millisecond):
					// call timed out
					fmt.Printf("[sendEntries2Peers] Raft: %v | Term: %v | Role: %v | Timeout\n", rf.me, rf.currentTerm, rf.role)
					replyChannel <- 0
					// TODO: retry if appendEntry failed
				}
			}
		}(i)
	}
	wg.Wait()
	var replyCount = 1
	for i := 0; i < lengthPeers-1; i++ {
		replyCount += <-replyChannel
	}
	fmt.Printf("[sendEntries2Peers] Raft: %v | Term: %v | Role: %v | replyCount: %v\n", rf.me, rf.currentTerm, rf.role, replyCount)

	if replyCount >= (len(rf.peers)+1)/2 {
		return true
	} else {
		return false
	}
}

type AppendApplyRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // Last commit index (?)
	PrevLogTerm  int // Term in PrevLogIndex
	LeaderCommit int
}

func (rf *Raft) RequestApply(req *AppendApplyRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	if req.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.role = Follower
	rf.currentTerm = req.Term

	// Reset election timer
	sleepSeconds := TIKER_SLEEP_SEC + (rand.Int63() % (TIKER_SLEEP_SEC))
	rf.electionTimer.Reset(time.Duration(sleepSeconds) * time.Millisecond)

	fmt.Printf("[RequestApply] Raft: %v | Term: %v | Role: %v | Commit Index: %v\n", rf.me, rf.currentTerm, rf.role, rf.commitIndex)

	if req.LeaderCommit > rf.commitIndex {
		// fail to copy
		reply.Success = false
	} else {
		rf.applyLog()
		reply.CurrentCommit = rf.commitIndex
	}
}

func (rf *Raft) sendApply(server int, args *AppendApplyRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestApply", args, reply)
	return ok
}

// sendApply2Peers sends the AppendEntriesRequest to all peers in the Raft cluster and waits for their replies.
// It returns the number of successful replies received.
// If resend is true, it will resend the entries to peers that have a lower commit index than the last applied index.
// Parameters:
//   - request: The AppendEntriesRequest to be sent to the peers.
//   - resend: A boolean indicating whether to resend entries if necessary.
//
// Returns:
//   - The number of successful replies received.
func (rf *Raft) sendApply2Peers(request *AppendApplyRequest, resend bool) int {
	lengthPeers := len(rf.peers)
	replyChannel := make(chan int, lengthPeers-1)

	var wg sync.WaitGroup
	wg.Add(lengthPeers)
	for i := 0; i < lengthPeers; i++ {
		go func(i int) {
			defer wg.Done()
			appendEntriesReply := AppendEntriesReply{}
			if i != rf.me {
				c := make(chan bool, 1)
				go func() {
					c <- rf.sendApply(i, request, &appendEntriesReply)
				}()
				select {
				case err := <-c:
					// use err and result
					fmt.Printf("[sendCommit2Peers %v] %v\n", rf.me, err)
					// Update nextIndex by reply if successful commit
					if appendEntriesReply.Success {
						rf.nextIndex[i] = appendEntriesReply.CurrentCommit
						replyChannel <- 1
					} else {
						replyChannel <- 0
					}
					if appendEntriesReply.Term > rf.currentTerm {
						rf.role = Follower
						rf.votedFor = -1
					}
					// TODO: resend
				case <-time.After(time.Duration(RPC_TIMEOUT_SEC) * time.Millisecond):
					// call timed out
					replyChannel <- 0
					// TODO: retry if appendEntry failed
				}
			}
		}(i)
	}
	wg.Wait()
	var replyCount = 1
	for i := 0; i < lengthPeers-1; i++ {
		replyCount += <-replyChannel
	}
	fmt.Printf("[sendCommit2Peers] Raft: %v | Term: %v | Role: %v | replyCount: %v\n", rf.me, rf.currentTerm, rf.role, replyCount)
	return replyCount
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("[Start] Raft: %v | Term: %v | Role: %v | Command size: %v | Command: %v\n", rf.me, rf.currentTerm, rf.role, len(fmt.Sprintf("%v", command)), command)

	// Your code here (2B).
	index := 0
	term := rf.currentTerm
	isLeader := (rf.role == Leader)

	if isLeader {
		// Update entry for leader
		entry := Entry{
			Term:  rf.currentTerm,
			Index: rf.commitIndex,
			Value: command,
		}

		// reset commit index
		rf.commitIndex = rf.lastApplied

		// check repeat command
		if len(rf.logEntries) > 0 && command == rf.logEntries[len(rf.logEntries)-1].Value {
			return index, term, isLeader
		}

		rf.logEntries = append(rf.logEntries, entry)
		rf.commitIndex++
		// Send entries to peers for applying
		isCommited := rf.sendEntries2Peers(true)

		// update leaderCommit
		if isCommited {
			// TODO: send apply to followers
			commitRequest := AppendApplyRequest{
				Term:         rf.currentTerm,
				LeaderCommit: rf.commitIndex,
			}
			appliedCount := rf.sendApply2Peers(&commitRequest, false)
			if appliedCount >= (len(rf.peers)+1)/2 {
				// Update Leader
				rf.applyLog()
			} else {
				// revert log commit
				rf.logEntries = rf.logEntries[:rf.commitIndex-1]
				rf.commitIndex = rf.lastApplied
			}
		} else {
			// revert log commit
			rf.logEntries = rf.logEntries[:rf.commitIndex-1]
			rf.commitIndex = rf.lastApplied
		}
		index = rf.commitIndex
		term = rf.currentTerm
	}

	fmt.Printf("[Start] Raft: %v | Term: %v | Role: %v | index, term, isLeader: %v, %v, %v\n", rf.me, rf.currentTerm, rf.role, index, term, isLeader)

	// return index, term, isLeader
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

func (rf *Raft) genEntriesReq(peerIndex int, currentFollowerCommit int) AppendEntriesRequest {
	fmt.Printf("[genEntries] Raft: %v | Term: %v | Role: %v | peerIndex: %v | rf.nextIndex[peerIndex]: %v | rf.commitIndex: %v\n", rf.me, rf.currentTerm, rf.role, peerIndex, rf.nextIndex[peerIndex], rf.commitIndex)
	// build request entry from currentFollowerCommit to last commit of leader
	var entries []Entry
	// TODO: generate AppendEntriesRequest by nextIndex
	for i := rf.nextIndex[peerIndex]; i < currentFollowerCommit; i++ {
		entries = append(entries, rf.logEntries[i])
	}
	// Obtain prevLogTerm
	prevLogTerm := -1
	if len(rf.logEntries) == 0 && rf.commitIndex != 0 {
		prevLogTerm = entries[rf.commitIndex-1].Term
	}
	resendEntries := AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.lastApplied,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	fmt.Printf("[genEntries] Raft: %v | Term: %v | Role: %v | size of entries: %v\n", rf.me, rf.currentTerm, rf.role, len(resendEntries.Entries))
	return resendEntries
}

func (rf *Raft) triggerHeartbeat() {
	result := rf.sendEntries2Peers(true)
	fmt.Printf("[triggerHeartbeat] Raft: %v | Term: %v | Role: %v | Result: %v\n", rf.me, rf.currentTerm, rf.role, result)
}

func (rf *Raft) triggerElection() {
	requestVoteArgs := RequestVoteArgs{
		Term:        rf.currentTerm + 1,
		CandidateId: rf.me,
		// TODO:
		LastLogIndex: 0,
		LastLogTerm:  rf.currentTerm,
	}

	rf.mu.Lock()
	rf.votedFor = rf.me
	// follower -> candidate
	rf.role = Candidate
	rf.mu.Unlock()

	lengthPeers := len(rf.peers)
	totalLengthChen := make(chan int, lengthPeers-1)
	var wg sync.WaitGroup
	wg.Add(lengthPeers)
	for i := 0; i < len(rf.peers); i++ {
		go func(i int) {
			defer wg.Done()
			requestVoteReply := RequestVoteReply{}
			if i != rf.me {
				c := make(chan bool, 1)
				go func() {
					c <- rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply)
				}()
				select {
				case err := <-c:
					// use err and result
					fmt.Printf("[triggerElection %v] %v\n", rf.me, err)
					if requestVoteReply.VoteGranted {
						totalLengthChen <- 1
					} else {
						fmt.Printf("[triggerElection] Raft: %v | Term: %v | Role: %v | fail by %v: %+v\n", rf.me, rf.currentTerm, rf.role, i, requestVoteReply)
						totalLengthChen <- 0
					}
				case <-time.After(time.Duration(RPC_TIMEOUT_SEC) * time.Millisecond):
					// call timed out
					fmt.Printf("[triggerElection] Raft: %v | Term: %v | Role: %v | Timeout\n", rf.me, rf.currentTerm, rf.role)
					totalLengthChen <- 0
				}
			}
		}(i)
	}
	wg.Wait()
	// How much votes have been obtained
	var voteCount = 1
	for i := 0; i < lengthPeers-1; i++ {
		voteCount += <-totalLengthChen
	}

	rf.mu.Lock()
	rf.currentTerm = requestVoteArgs.Term
	if voteCount >= (len(rf.peers)+1)/2 {
		rf.role = Leader
		rf.triggerHeartbeat()
	} else {
		rf.role = Follower
		rf.votedFor = -1
	}
	rf.mu.Unlock()
	fmt.Printf("[triggerElection] Raft: %v | Term: %v | Role: %v | voteCount: %v, threshold: %v\n", rf.me, rf.currentTerm, rf.role, voteCount, (len(rf.peers)+1)/2)
}

func (rf *Raft) heartbeater() {
	defer rf.heartbeatTimer.Stop()

	for rf.killed() == false {
		select {
		case <-rf.heartbeatTimer.C:
			sleepSeconds := HEATHBEAT_SLEEP_SEC + (rand.Int63() % (HEATHBEAT_SLEEP_SEC))
			rf.heartbeatTimer.Reset(time.Duration(sleepSeconds) * time.Millisecond)
			if rf.role == Leader {
				rf.triggerHeartbeat()
				fmt.Printf("[heartbeater] Raft: %v | Term: %v | Role: %v\n", rf.me, rf.currentTerm, rf.role)
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	defer rf.electionTimer.Stop()

	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			sleepSeconds := TIKER_SLEEP_SEC + (rand.Int63() % (TIKER_SLEEP_SEC))
			rf.electionTimer.Reset(time.Duration(sleepSeconds) * time.Millisecond)
			if rf.role != Leader {
				// TODO: Pre Election
				// rf.triggerPreElection()
				rf.triggerElection()
				fmt.Printf("[ticker] Raft: %v | Term: %v | Role: %v\n", rf.me, rf.currentTerm, rf.role)
			}
		}
	}
}

// applyLog applies the log entries to the state machine.
// It continuously checks for the commit index and applies the corresponding log entry.
// It also sends the applied log entry to the apply channel.
func (rf *Raft) applyLog() {
	fmt.Printf("[applyLog] Raft: %v | Term: %v | Role: %v | lastApplied: %v | rf.commitIndex: %v | log size: %v\n", rf.me, rf.currentTerm, rf.role, rf.lastApplied, rf.commitIndex, len(rf.logEntries))
	i := rf.commitIndex
	if i <= 0 || i > len(rf.logEntries) {
		return
	}

	log := rf.logEntries[i-1]
	if log.Index > rf.commitIndex {
		return
	}

	msg := ApplyMsg{CommandValid: true, Command: log.Value, CommandIndex: log.Index + 1}
	select {
	case rf.applyCh <- msg:
		fmt.Printf("[applyLog] Raft: %v | Term: %v | Role: %v | lastApplied: %v | msg.CommandIndex: %v\n", rf.me, rf.currentTerm, rf.role, rf.lastApplied, msg.CommandIndex)
		// TODO: Check rf.lastApplied
		rf.lastApplied = Max(rf.lastApplied, msg.CommandIndex)
	}
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// 2A: initialize
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower

	// 2B: log entries
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	// TODO:
	rf.matchIndex = make([]int, len(peers))
	// TODO: Add a empty entry in logEntries when initialation

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	sleepSeconds := TIKER_SLEEP_SEC + (rand.Int63() % (TIKER_SLEEP_SEC))
	rf.electionTimer = time.NewTicker(time.Duration(sleepSeconds) * time.Millisecond)

	sleepSeconds = HEATHBEAT_SLEEP_SEC + (rand.Int63() % (HEATHBEAT_SLEEP_SEC))
	rf.heartbeatTimer = time.NewTicker(time.Duration(sleepSeconds) * time.Millisecond)

	go rf.heartbeater()
	go rf.ticker()
	go rf.applyLog()

	return rf
}
