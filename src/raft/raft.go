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
	// "fmt"
	"sync"
	"sync/atomic"
    "time" 
    "math/rand"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type state string 
const (
    LEADER    state = "LEADER"
    FOLLOWER  state = "FOLLOWER"
    CANDIDATE state = "CANDIDATE"
)

const (
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    currentTerm int // latest term the server has seen (initialized to 0 on 
                       // on first boot and increase monotonically)
    votedFor int // might need to make this signed because can be null 
                    // if did not vote
    currentState state
    electionTimeout time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    DPrintf("%d GetState locked\n", rf.me)
    term = rf.currentTerm
    isleader = (rf.currentState == LEADER)
    rf.mu.Unlock()
    DPrintf("%d GetState unlocked\n", rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

// function that resets the election timer
// heartbeats are sent every 100 milliseconds
// election timeout should be >= n * (heartbeat interval) where n is a small 
// constant. election timeout should be small enough that we can choose a new 
// leader within 5 seconds but big enough to the point where random intervals 
// between two timers should big enough that it allows all RPCs to be sent 
// and processes (rtm said around 10 milliseconds in lecture..)
func (rf *Raft) resetElectionTimer() {
    lowerBound := (5 * 100) // 500 milliseconds
    upperBound := int(2.5 * 1000) // 2.5 seconds = 2500 milliseconds
    randomTimeout := rand.Intn(upperBound - lowerBound) + lowerBound
    rf.electionTimeout = time.Now().Add(time.Duration(randomTimeout) * time.Millisecond)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int 
    CandidateId int 
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int 
    VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

    // "If a step says “reply false”, this means you should reply immediately, 
    // and not perform any of the subsequent steps."" - 
    // https://thesquareplanet.com/blog/students-guide-to-raft/

    // "If a server receives a request with a stale term 
    // number, it rejects the request"
    rf.mu.Lock()
    DPrintf("%d RequestVote Locked\n", rf.me)
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        rf.mu.Unlock()
        return
    }

    // if i am out of date, convert to follower right away
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.currentState = FOLLOWER
        DPrintf("%d converted to follower in RequestVote Handler\n", rf.me)
    }

    // if did not vote for anyone this current term, I can vote
    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        reply.Term = args.Term
        reply.VoteGranted = true
        rf.votedFor = args.CandidateId
        rf.resetElectionTimer()
    }
    rf.mu.Unlock()
    DPrintf("%d RequestVote unlocked\n", rf.me)
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) becomeLeader() {
    rf.currentState = LEADER
    go rf.leaderSendHeartbeatsRoutine()
}

// we seperate this function into its own go routine so that the reply can be 
// processed independently from other RequestVotes send to other peers
func (rf *Raft) sendRequestVoteAndProcessReplyRoutine(server int, 
                                                      args *RequestVoteArgs,
                                                      reply *RequestVoteReply, 
                                                      voteCount *int,
                                                      wg *sync.WaitGroup) {
    defer wg.Done()
    ok := rf.sendRequestVote(server, args, reply)

    rf.mu.Lock()
    DPrintf("%d sendRequestVoteAndProcessReplyRoutine Locking\n", rf.me)
    if ok {
        // if I discover a higher term, convert to follower, cannot be candidate
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.votedFor = -1
            rf.currentState = FOLLOWER
            DPrintf("%d converted to follower in sendRequestVoteAndProcessReplyRoutine\n", rf.me)
        }
        // if I am still a candidate and the vote was granted, then count++
        if rf.currentState == CANDIDATE && reply.VoteGranted { 
            (*voteCount)++
            if *voteCount > len(rf.peers)/2 {
                rf.becomeLeader()
                DPrintf("%d became leader in sendRequestVoteAndProcessReplyRoutine\n", rf.me)
            }
        }
    }
    rf.mu.Unlock()
    DPrintf("%d sendRequestVoteAndProcessReplyRoutine unlocked\n", rf.me)
}

func (rf *Raft) startElection() {
    rf.mu.Lock()
    DPrintf("%d locked in startElection\n", rf.me)
    DPrintf("%d starting election\n", rf.me)
    rf.currentTerm++
    rf.votedFor = rf.me
    rf.resetElectionTimer()
    rf.mu.Unlock()
    DPrintf("%d unlocked in startElection\n", rf.me)

    voteCount := 1 // voted for itself
    wg := new(sync.WaitGroup)
    rf.mu.Lock()
    DPrintf("%d locked in startElection\n", rf.me)
    savedCurrentTerm := rf.currentTerm
    rf.mu.Unlock()
    DPrintf("%d unlocked in startElection\n", rf.me)
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        args := &RequestVoteArgs {
            Term: savedCurrentTerm,
            CandidateId: rf.me,
        }
        reply := &RequestVoteReply {
        }
        wg.Add(1)
        go rf.sendRequestVoteAndProcessReplyRoutine(i, args, reply, &voteCount, wg)
    }
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
    Term int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
    Term int
    Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    DPrintf("%d AppendEntires RPC Trying to Acquire Lock\n", rf.me)
    rf.mu.Lock()
    DPrintf("%d AppendEntires RPC locked\n", rf.me)
    DPrintf("%d handling heartbeat RPCs\n", rf.me)
    DPrintf("%d args term: %d, my term: %d\n", rf.me, args.Term, rf.currentTerm)
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
    } else {
        reply.Term = args.Term
        reply.Success = true
        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            rf.votedFor = -1
            rf.currentState = FOLLOWER
            DPrintf("%d converted to follower in AppendEntries handler\n", rf.me)
        }
        rf.resetElectionTimer()
    }
    rf.mu.Unlock()
    DPrintf("%d AppendEntires RPC Unlocked\n", rf.me)
}

func (rf *Raft) sendAppendEntriesAndProcessReplyRoutine(server int,
                                                        args *AppendEntriesArgs,
                                                        reply *AppendEntriesReply) {
    DPrintf("%d keep sending to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) 
    DPrintf("%d sendAppendEntries Ok or not: %t\n", rf.me, ok)
    DPrintf("%d finished sendsing heartbeat to %d\n", rf.me, server)
    if !ok {
        return
    }
    if !reply.Success {
        rf.mu.Lock()
        DPrintf("%d Locked in sendAppendEntriesAndProcessReplyRoutine %d\n", rf.me, server)
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
        }
        rf.votedFor = -1
        rf.currentState = FOLLOWER
        rf.mu.Unlock()
        DPrintf("%d Unlocked in sendAppendEntriesAndProcessReplyRoutine %d\n", rf.me, server)
    }
}

func (rf *Raft) leaderSendHeartbeatsRoutine() {
	for {
        rf.mu.Lock()
        DPrintf("%d locked in leaderSendHeartbeatsRoutine 1\n", rf.me)
        if rf.currentState != LEADER {
            rf.mu.Unlock()
            DPrintf("%d unlocked in leaderSendHeartbeatsRoutine 1\n", rf.me)
            break
        }
        // send heartbeats every 1/10th of second. 
        for i := 0; i < len(rf.peers); i++ {
            if i == rf.me {
                continue;
            }
            args := &AppendEntriesArgs{
                Term: rf.currentTerm, 
            }
            reply := &AppendEntriesReply{
            }
            go rf.sendAppendEntriesAndProcessReplyRoutine(i, args, reply)
        }
        DPrintf("%d sending heartbeat RPCS\n", rf.me)
        rf.mu.Unlock()
        DPrintf("%d unlocked in leaderSendHeartbeatsRoutine 2\n", rf.me)
        // process the replies
        // if still valid leader, we sleep, else we would immediately end this routine
        time.Sleep(100 * time.Millisecond)
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed()  {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
        
        // "Perhaps the simplest plan is to maintain a variable in the
        // Raft struct containing the last time at which the peer heard from the
        // leader, and to have the election timeout goroutine periodically check
        // to see whether the time since then is greater than the timeout period.
        // It's easiest to use time.Sleep() with a small constant argument to
        // drive the periodic checks."
        rf.mu.Lock()
        DPrintf("%d locked in ticker\n", rf.me)
        if (rf.currentState == FOLLOWER || rf.currentState == CANDIDATE) && time.Now().After(rf.electionTimeout) {
            rf.currentState = CANDIDATE
            DPrintf("%d converted to candidate in ticker\n", rf.me)
            rf.mu.Unlock()
            DPrintf("%d unlocked in ticker\n", rf.me)
            rf.startElection()
        } else {
            rf.mu.Unlock()
            DPrintf("%d unlocked in ticker\n", rf.me)
        }
        time.Sleep(100 * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.currentState = FOLLOWER // everyone starts off as follower
    rf.resetElectionTimer() // everyone's election timer starts randomly


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
