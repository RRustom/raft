package raft

import (
  "fmt"
  "log"
  "math/rand"
  "os"
  "sync"
  "sync/atomic"
  "time"
)

const DebugCM = 1

type LogEntry struct {
  Command interface{}
  Term int
}

// data reported by Raft to the commit channel.
// each commit entry notifies the client that consensus was reached on a command
// and it can be applied to the cleint's state machine
type CommitEntry struct {
  // client command being committed
  Command interface{}

  // log index at which the cleint command is committed
  Index int

  // raft term at which the client command is committed
  Term int
}

type CMState int

const (
  Follower CMState = iota
  Candidate
  Leader
  Dead
)

func (s CMState) String() string {
  switch s {
  case Follower:
    return "Follower"
  case Candidate:
    return "Candidate"
  case Leader:
    return "Leader"
  case Dead:
    return "Dead"
  default:
    panic("unreachable")
  }
}

// ConsensusModule (CM) is a single node in raft
type ConsensusModule struct {
  // protects concurrent access to a CM
  mu sync.Mutex

  // server ID of this CM
  id int

  // IDs of peers in the cluster
  peerIds []int

  // server containing this CM. Issues RPC calls to peers
  server *Server

  // channel where this CM is going to report committed log entries. It's passed
  // in by the client during construction
  commitChan chan <- CommitEntry

  // internal notification channel used by goroutines that commit new entries to the log
  // to notify that these entries may be sent on commitChan
  newCommitReadyChan chan struct{}

  // persistant Raft state on all servers
  currentTerm int
  votedFor int
  log []LogEntry

  // volatile raft state on all servers
  commitIndex int
  lastApplied int
  state CMState
  // any event that terminates an election
  // (ex: valid heartbeat received, vote given to another candidate)
  electionResetEvent time.Time

  // volatile raft state on leaders
  nextIndex map[int]int
  matchIndex map[int]int
}

// debugging message
// func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
//
// }

type RequestVoteArgs struct {
  Term          int
  CandidateId   int
  LastLogIndex  int
  LastLogTerm   int
}

type RequestVoteReply struct {
  Term        int
  VoteGranted bool
}

type AppendEntriesArgs struct {
  Term          int
  LeaderId      int

  PrevLogIndex  int
  PrevLogTerm   int
  Entries       []LogEntry
  LeaderCommit  int
}

type AppendEntriesReply struct {
  Term    int
  Success bool
}

// create a new CM with the given ID, peers, and server
// ready channel signals the CM that all peers are connected, and that it is safe
// to start its state machine
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan <- CommitEntry) *ConsensusModule {
  cm := new(ConsensusModule)
  cm.id = id
  cm.peerIds = peerIds
  cm.server = server
  cm.commitChan = commitChan
  cm.newCommitReadyChan = make(chan struct{}, 16)
  cm.state = Follower
  cm.votedFor = -1
  cm.commitIndex = -1
  cm.lastApplied = -1
  cm.nextIndex = make(map[int]int)
  cm.matchIndex = make(map[int]int)

  go func() {
    // CM is dormant until ready is signaled. Then it starts election timer
    <-ready
    cm.mu.Lock()
    cm.electionResetEvent = time.Now()
    cm.mu.Unlock()
    cm.runElectionTimer()
  }()

  go cm.commitChanSender()
  return cm
}

func (cm *ConsensusModule) Stop() {
  cm.mu.Lock()
  defer cm.mu.Unlock()
  cm.state = Dead
  cm.dlog("becomes Dead")
  close(cm.newCommitReadyChan)
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
  if DebugCM > 0 {
    format = fmt.Sprintf("[%d] ", cm.id) + format
    log.Printf(format, args...)
  }
}

// report state of this CM
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
  cm.mu.Lock()
  defer cm.mu.Unlock()
  return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Submit(command interface{}) bool {
  cm.mu.Lock()
  defer cm.mu.Unlock()

  cm.dlog("Submit received by %v: %v", cm.state, command)
  if cm.state == Leader {
    cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
    cm.dlog("... log=%v", cm.log)
    return true
  }
  return false
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
  cm.mu.Lock()
  defer cm.mu.Unlock()
  if cm.state == Dead {
    return nil
  }
  lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
  cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%dm log index/iterm=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

  if args.Term > cm.currentTerm {
    cm.dlog("... term out of date in RequestVote")
    cm.becomeFollower(args.Term)
  }

  // if caller's term aligns with ours, and we haven't voted for any candidates yet,
  // then vote
  if cm.currentTerm == args.Term &&
  (cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
  (args.LastLogTerm > lastLogTerm ||
    (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
    reply.VoteGranted = true
    cm.votedFor = args.CandidateId
    cm.electionResetEvent = time.Now()
  } else {
    reply.VoteGranted = false
  }
  reply.Term = cm.currentTerm
  cm.dlog("... RequestVote reply: %+v", reply)
  return nil
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
  cm.mu.Lock()
  defer cm.mu.Unlock()
  if cm.state == Dead {
    return nil
  }
  cm.dlog("AppendEntries: %+v", args)

  if args.Term > cm.currentTerm {
    cm.dlog("... term out of date in AppendEntries")
    cm.becomeFollower(args.Term)
  }

  reply.Success = false
  if args.Term == cm.currentTerm {
    if cm.state != Follower {
      cm.becomeFollower(args.Term)
    }
    cm.electionResetEvent = time.Now()

    // does log contain an entry at PrevLogIndex whose term matches PrevLogTerm?
    // if PrevLogIndex = -1, this is trivially true
    if args.PrevLogIndex == -1 ||
      (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
      reply.Success = true

      // find an insertion point where there is a term mismatch between the
      // existing log starting at PrevLogIndex + 1 and the new entries sent
      // in the RPC
      logInsertIndex := args.PrevLogIndex + 1
      newEntriesIndex := 0

      for {
        if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
          break
        }
        if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
          break
        }
        logInsertIndex++
        newEntriesIndex++
      }

      // at the end of this loop:
      // - logInsertIndex points at the end of the log, or an index where the term
      //  mismatches with an entry from the leader
      // - newEntriesIndex points at the end of Entries, or an index where the term
      //  mismatches with the corresponding log entry
      if newEntriesIndex < len(args.Entries) {
        cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
        cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
        cm.dlog("... log is now: %v", cm.log)
      }

      // set commit index
      if args.LeaderCommit > cm.commitIndex {
        cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
        cm.dlog("... setting commitIndex=%d", cm.commitIndex)
        cm.newCommitReadyChan <- struct{}{}
      }
    }
  }

  reply.Term = cm.currentTerm
  cm.dlog("AppendEntries reply: %+v", *reply)
  return nil
}

// election timer
func (cm *ConsensusModule) runElectionTimer() {
  timeoutDuration := cm.electionTimeout()
  cm.mu.Lock()
  termStarted := cm.currentTerm
  cm.mu.Unlock()
  cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

  // loop until either:
  // 1. we discover election timer is no longer needed
  // 2. election timer expires and this CM becomes a candidate
  // For a follower, this runs in the background for the entire lifetime
  ticker := time.NewTicker(10 * time.Millisecond)
  defer ticker.Stop()
  for {
    <-ticker.C

    // if state or term is incorrect, terminate election timer
    cm.mu.Lock()
    if cm.state != Candidate && cm.state != Follower {
      cm.dlog("in election timer state=%s, bailing out", cm.state)
      cm.mu.Unlock()
      return
    }

    if termStarted != cm.currentTerm {
      cm.dlog("in election timer, term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
      cm.mu.Unlock()
      return
    }

    if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
      cm.startElection()
      cm.mu.Unlock()
      return
    }
    cm.mu.Unlock()
  }
}

// what we need to run an election:
// 1. switch state to candidate, increment term
// 2. send RV RPCs to peers, asking for a vote in this election
// 3. Wait for replies to RPCs, count if we get enough votes to become leader

// start a new election with this CM as a candidate
// need cm.mu to be locked
func (cm *ConsensusModule) startElection() {
  cm.state = Candidate
  cm.currentTerm += 1
  savedCurrentTerm := cm.currentTerm
  cm.electionResetEvent = time.Now()
  cm.votedFor = cm.id
  cm.dlog("becomes Canddiate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

  // candidate votes for itself
  var votesReceived int32 = 1

  // Send RequestVote RPCs to peers
  for _, peerId := range cm.peerIds {
    go func(peerId int) {
      cm.mu.Lock()
      savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
      cm.mu.Unlock()

      args := RequestVoteArgs{
        Term: savedCurrentTerm,
        CandidateId: cm.id,
        LastLogIndex: savedLastLogIndex,
        LastLogTerm: savedLastLogTerm,
      }

      cm.dlog("sending RequestVote to %d: %+v", peerId, args)
      var reply RequestVoteReply

      // each RPC issued in a separate goroutine (since RPC calls synchronous)
      // QUESTION: why are we using pointers for reply?
      if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
        cm.mu.Lock()
        defer cm.mu.Unlock()
        cm.dlog("received RequestVoteReply %+v", reply)

        // if RPC succeeds, some time has passed, so check current state
        // if no longer candidate, bail
        // (can happen if we won election because enough votes collected in other RPC calls
        // or we switched back to being a follower if one of the other RPC calls heard
        // from a server with a higher term
        if cm.state != Candidate {
          cm.dlog("while waiting for reply, state = %v", cm.state)
          return
        }

        // if still candidate
        if reply.Term > savedCurrentTerm {
          // other candidate one while we were collecting votes
          cm.dlog("term out of date in RequestVoteReply")
          cm.becomeFollower(reply.Term)
          return
        } else if reply.Term == savedCurrentTerm {
          if reply.VoteGranted {
            // use atomic to collect votes from multiple goroutines safely
            votes := int(atomic.AddInt32(&votesReceived, 1))
            if votes*2 > len(cm.peerIds) + 1 {
              // won the election
              cm.dlog("wins election with %d votes", votes)
              cm.startLeader()
              return
            }
          }
        }
      }
    }(peerId)
  }

  // in case this election is not successful, run another election timer
  go cm.runElectionTimer()
}

// becomeFollower
func (cm *ConsensusModule) becomeFollower(term int) {
  cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
  cm.state = Follower
  cm.currentTerm = term
  cm.votedFor = -1
  cm.electionResetEvent = time.Now()

  // followers always have election timer in background
  go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
  cm.state = Leader
  cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)

  go func() {
    ticker := time.NewTicker(50 * time.Millisecond)
    defer ticker.Stop()

    // send periodic heartbeats, as long as still leader
    for {
      cm.leaderSendHeartbeats()
      <- ticker.C

      cm.mu.Lock()
      if cm.state != Leader {
        cm.mu.Unlock()
        return
      }
      cm.mu.Unlock()
    }
  }()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
  cm.mu.Lock()
  savedCurrentTerm := cm.currentTerm
  cm.mu.Unlock()

  for _, peerId := range cm.peerIds {
    go func(peerId int) {
      cm.mu.Lock()
      ni := cm.nextIndex[peerId]
      prevLogIndex := ni - 1
      prevLogTerm := -1
      if prevLogIndex >= 0 {
        prevLogTerm = cm.log[prevLogIndex].Term
      }
      entries := cm.log[ni:]

      args := AppendEntriesArgs{
        Term: savedCurrentTerm,
        LeaderId: cm.id,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm: prevLogTerm,
        Entries: entries,
        LeaderCommit: cm.commitIndex,
      }

      cm.mu.Unlock()
      cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
      var reply AppendEntriesReply
      err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply)
      if err == nil {
        cm.mu.Lock()
        defer cm.mu.Unlock()
        if reply.Term > savedCurrentTerm {
          cm.dlog("term out of date in heartbeat reply")
          cm.becomeFollower(reply.Term)
          return
        }

        if cm.state == Leader && savedCurrentTerm == reply.Term {
          if reply.Success {
            cm.nextIndex[peerId] = ni + len(entries) // QUESTION??
            cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
            cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)

            savedCommitIndex := cm.commitIndex
            for i := cm.commitIndex + 1; i < len(cm.log); i++ {
              if cm.log[i].Term == cm.currentTerm {
                matchCount := 1
                for _, peerId := range cm.peerIds {
                  if cm.matchIndex[peerId] >= i {
                    matchCount++
                  }
                }
                if matchCount*2 > len(cm.peerIds)+1 {
                  cm.commitIndex = i
                }
              }
            }
            if cm.commitIndex != savedCommitIndex {
              cm.dlog("leader sets commitIndex :=%d", cm.commitIndex)
              cm.newCommitReadyChan <- struct{}{}
            }
          } else {
            cm.nextIndex[peerId] = ni - 1
            cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
          }
        }
      }
    }(peerId)
  }
}

// return the last log index and the last log entry's term
// (or -1 if there is no log) for this server
// cm.mu must be locked
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
  if len(cm.log) > 0 {
    lastIndex := len(cm.log) - 1
    return lastIndex, cm.log[lastIndex].Term
  } else {
    return -1, -1
  }
}


func (cm *ConsensusModule) commitChanSender() {
  for range cm.newCommitReadyChan {
    // find which entries need to be applied
    cm.mu.Lock()
    savedTerm := cm.currentTerm
    savedLastApplied := cm.lastApplied
    var entries []logEntry
    if cm.commitIndex > cm.lastApplied {
      entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
      cm.lastApplied = cm.commitIndex
    }
    cm.mu.Unlock()
    cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

    for i, entry := range entries {
      cm.commitChan <- CommitEntry{
        Command: entry.Command,
        Index: savedLastApplied + i + 1,
        Term: savedTerm,
      }
    }
  }
  cm.dlog("commitChanSender done")
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
  // If RAFT_FORCE_MORE_REELECTION is set, stress-test by generating a
  // hard-coded number very often
  // Creates collisions between different servers and force more re-elections
  if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
    return time.Duration(150) * time.Millisecond
  } else {
    return time.Duration(150+rand.Intn(150)) * time.Millisecond
  }
}

func intMin(a, b int) int {
  if a < b {
    return a
  }
  return b
}
