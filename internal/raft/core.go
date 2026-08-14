package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// Role is a node's current position in the Raft state machine (paper
// §5, Figure 4).
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Config wires a Core to its dependencies. Every field that touches the
// outside world (network, disk, time, randomness) is an injected
// interface or explicit value, so Core itself never touches real
// infrastructure directly and is fully testable in-process.
type Config struct {
	ID    string
	Peers []string // other node IDs, not including Self

	Transport Transport
	Clock     Clock
	Rand      *rand.Rand // source for election-timeout jitter; inject a seeded one for reproducible tests

	HardState *storage.HardStateStore
	Log       *storage.Log // present but not yet driven by replication until Phase 3

	ElectionTimeoutMin time.Duration // paper default 150ms; set Min == Max for a fixed (non-jittered) timeout
	ElectionTimeoutMax time.Duration // paper default 300ms
	HeartbeatInterval  time.Duration // must be well below ElectionTimeoutMin
}

// Snapshot is a point-in-time, race-free copy of a Core's externally
// visible state, obtained via Core.State().
type Snapshot struct {
	Role        Role
	CurrentTerm uint64
	VotedFor    string
	LeaderID    string
}

type voteRequest struct {
	args  *proto.RequestVoteArgs
	reply chan *proto.RequestVoteReply
}

type appendRequest struct {
	args  *proto.AppendEntriesArgs
	reply chan *proto.AppendEntriesReply
}

type voteResult struct {
	term     uint64 // the term this vote was requested for
	granted  bool
	peerTerm uint64
}

type stateQuery struct {
	reply chan Snapshot
}

// Core is a single Raft node's consensus state machine. All fields
// below the channels are touched exclusively by the goroutine running
// Run — no mutex is needed around them, by construction. Every other
// goroutine (RPC delivery, election-timeout firing, vote tallying,
// state queries) communicates with Core only through its channels.
type Core struct {
	cfg Config

	role        Role
	currentTerm uint64
	votedFor    string
	leaderID    string

	votesReceived int

	voteReqCh    chan voteRequest
	appendReqCh  chan appendRequest
	voteResCh    chan voteResult
	termUpdateCh chan uint64
	stateCh      chan stateQuery
	stopCh       chan struct{}
	stopOnce     sync.Once

	// electionTimer is created here, synchronously, in NewCore rather
	// than inside Run — Run executes on its own goroutine, and a caller
	// (e.g. a FakeClock-driven test) may advance the clock immediately
	// after starting that goroutine, before it has necessarily reached
	// its first statement. Creating the timer during NewCore guarantees
	// it is already registered with Clock before Run is ever scheduled,
	// so there is no startup race between "the goroutine hasn't created
	// its timer yet" and "the test just advanced past the deadline."
	electionTimer Timer
}

// NewCore constructs a Core and loads its persisted hard state, so a
// node restarted after a crash resumes at the term/vote it left off at
// rather than forgetting it already voted this term (Raft paper
// §5.4.1's safety requirement for persistent state).
func NewCore(cfg Config) *Core {
	hs, err := cfg.HardState.Load()
	if err != nil {
		panic(fmt.Sprintf("raft: failed to load hard state: %v", err))
	}
	c := &Core{
		cfg:          cfg,
		role:         Follower,
		currentTerm:  hs.CurrentTerm,
		votedFor:     hs.VotedFor,
		voteReqCh:    make(chan voteRequest),
		appendReqCh:  make(chan appendRequest),
		voteResCh:    make(chan voteResult),
		termUpdateCh: make(chan uint64),
		stateCh:      make(chan stateQuery),
		stopCh:       make(chan struct{}),
	}
	c.electionTimer = cfg.Clock.NewTimer(c.randomizedElectionTimeout())
	return c
}

// Run drives the Core's state machine until Stop is called. Call it in
// its own goroutine.
func (c *Core) Run() {
	electionTimer := c.electionTimer
	defer electionTimer.Stop()
	var heartbeatTicker Ticker
	// heartbeatTicker is created and replaced dynamically (only while
	// Leader), so it can't be deferred directly like electionTimer — a
	// plain `defer heartbeatTicker.Stop()` here would capture the nil
	// value from this line, not whatever it's set to later. This
	// closure re-reads the variable at return time instead. Without
	// this, a leader's ticker would stay registered (and active) with
	// Clock forever after Stop(), and a FakeClock's AdvanceAndSync would
	// block forever trying to deliver a fire nothing is left to receive.
	defer func() {
		if heartbeatTicker != nil {
			heartbeatTicker.Stop()
		}
	}()

	for {
		select {
		case req := <-c.voteReqCh:
			reply, resetTimer := c.handleRequestVote(req.args)
			req.reply <- reply
			if resetTimer {
				electionTimer.Reset(c.randomizedElectionTimeout())
			}

		case req := <-c.appendReqCh:
			reply, resetTimer := c.handleAppendEntries(req.args)
			req.reply <- reply
			if resetTimer {
				electionTimer.Reset(c.randomizedElectionTimeout())
			}
			if c.role != Leader && heartbeatTicker != nil {
				heartbeatTicker.Stop()
				heartbeatTicker = nil
			}

		case <-electionTimer.C():
			c.startElection()
			electionTimer.Reset(c.randomizedElectionTimeout()) // a failed/split election must also time out and retry

		case res := <-c.voteResCh:
			if c.tallyVote(res) {
				electionTimer.Stop()
				heartbeatTicker = c.cfg.Clock.NewTicker(c.cfg.HeartbeatInterval)
				c.broadcastHeartbeats() // send the first heartbeat immediately, don't wait a full interval
			}

		case <-tickerChan(heartbeatTicker):
			c.broadcastHeartbeats()

		case peerTerm := <-c.termUpdateCh:
			if peerTerm > c.currentTerm {
				c.stepDown(peerTerm)
				electionTimer.Reset(c.randomizedElectionTimeout())
				if heartbeatTicker != nil {
					heartbeatTicker.Stop()
					heartbeatTicker = nil
				}
			}

		case q := <-c.stateCh:
			q.reply <- c.snapshot()

		case <-c.stopCh:
			return
		}
	}
}

// Stop terminates Run. Safe to call any number of times, including
// concurrently, and both before and after a test's t.Cleanup also calls
// it — only the first call has any effect.
func (c *Core) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

func tickerChan(t Ticker) <-chan time.Time {
	if t == nil {
		return nil // a nil channel blocks forever in select: "not ticking yet"
	}
	return t.C()
}

// State returns a race-free snapshot of Core's current state via a
// synchronous round trip through Core's own goroutine. If Core has
// already stopped, it returns the zero Snapshot rather than blocking
// forever.
func (c *Core) State() Snapshot {
	reply := make(chan Snapshot)
	select {
	case c.stateCh <- stateQuery{reply: reply}:
	case <-c.stopCh:
		return Snapshot{}
	}
	select {
	case s := <-reply:
		return s
	case <-c.stopCh:
		return Snapshot{}
	}
}

func (c *Core) snapshot() Snapshot {
	return Snapshot{Role: c.role, CurrentTerm: c.currentTerm, VotedFor: c.votedFor, LeaderID: c.leaderID}
}

// HandleRequestVote implements RPCHandler for inbound RequestVote RPCs.
func (c *Core) HandleRequestVote(args *proto.RequestVoteArgs) *proto.RequestVoteReply {
	reply := make(chan *proto.RequestVoteReply)
	select {
	case c.voteReqCh <- voteRequest{args: args, reply: reply}:
	case <-c.stopCh:
		return nil
	}
	select {
	case r := <-reply:
		return r
	case <-c.stopCh:
		return nil
	}
}

// HandleAppendEntries implements RPCHandler for inbound AppendEntries
// RPCs.
func (c *Core) HandleAppendEntries(args *proto.AppendEntriesArgs) *proto.AppendEntriesReply {
	reply := make(chan *proto.AppendEntriesReply)
	select {
	case c.appendReqCh <- appendRequest{args: args, reply: reply}:
	case <-c.stopCh:
		return nil
	}
	select {
	case r := <-reply:
		return r
	case <-c.stopCh:
		return nil
	}
}

// randomizedElectionTimeout picks a randomized election timeout in
// [ElectionTimeoutMin, ElectionTimeoutMax] (Raft paper §5.2: randomized
// timeouts make split votes rare and, when they do happen, quick to
// resolve). If Min == Max, the timeout is fixed and no randomness is
// consumed — this is what lets tests assert exact-boundary behavior
// deterministically.
func (c *Core) randomizedElectionTimeout() time.Duration {
	span := c.cfg.ElectionTimeoutMax - c.cfg.ElectionTimeoutMin
	if span <= 0 {
		return c.cfg.ElectionTimeoutMin
	}
	jitter := time.Duration(c.cfg.Rand.Int63n(int64(span) + 1))
	return c.cfg.ElectionTimeoutMin + jitter
}

// startElection converts this node to Candidate and requests votes from
// every peer (Raft paper §5.2).
func (c *Core) startElection() {
	c.role = Candidate
	c.currentTerm++
	c.votedFor = c.cfg.ID
	c.votesReceived = 1 // vote for self
	c.leaderID = ""
	c.persistHardState()

	args := &proto.RequestVoteArgs{
		Term:         c.currentTerm,
		CandidateID:  c.cfg.ID,
		LastLogIndex: c.cfg.Log.LastIndex(),
		LastLogTerm:  c.cfg.Log.LastTerm(),
	}
	for _, peer := range c.cfg.Peers {
		peer := peer
		go func() {
			reply, err := c.cfg.Transport.RequestVote(peer, args)
			if err != nil {
				return
			}
			c.voteResCh <- voteResult{term: args.Term, granted: reply.VoteGranted, peerTerm: reply.Term}
		}()
	}

	// A single-node cluster (no peers) wins its own election immediately.
	if len(c.cfg.Peers) == 0 {
		c.becomeLeader()
	}
}

// tallyVote folds one RequestVote reply into the current election.
// Returns true exactly on the transition to Leader.
func (c *Core) tallyVote(res voteResult) bool {
	if res.peerTerm > c.currentTerm {
		c.stepDown(res.peerTerm)
		return false
	}
	if c.role != Candidate || res.term != c.currentTerm {
		return false // stale reply from a prior or since-abandoned election round
	}
	if !res.granted {
		return false
	}
	c.votesReceived++
	if c.votesReceived*2 > len(c.cfg.Peers)+1 {
		c.becomeLeader()
		return true
	}
	return false
}

func (c *Core) becomeLeader() {
	c.role = Leader
	c.leaderID = c.cfg.ID
	// nextIndex/matchIndex initialization arrives with log replication in Phase 3.
}

// stepDown reverts to Follower upon observing a higher term from any
// RPC or RPC reply (Raft paper §5.1: "If a candidate or leader
// discovers that its term is out of date, it immediately reverts to
// follower state").
func (c *Core) stepDown(term uint64) {
	c.currentTerm = term
	c.role = Follower
	c.votedFor = ""
	c.leaderID = ""
	c.persistHardState()
}

// handleRequestVote implements the RequestVote RPC receiver logic
// (Raft paper §5.2, Figure 2). Returns whether granting the vote
// should reset this node's election timer — only a granted vote does;
// merely receiving a RequestVote (e.g. from a candidate we reject) must
// not reset it, or a partitioned or disruptive candidate could keep
// interrupting an otherwise-healthy cluster's election timers without
// ever earning a vote.
func (c *Core) handleRequestVote(args *proto.RequestVoteArgs) (*proto.RequestVoteReply, bool) {
	if args.Term > c.currentTerm {
		c.stepDown(args.Term)
	}
	if args.Term < c.currentTerm {
		return &proto.RequestVoteReply{Term: c.currentTerm, VoteGranted: false}, false
	}

	logOK := args.LastLogTerm > c.cfg.Log.LastTerm() ||
		(args.LastLogTerm == c.cfg.Log.LastTerm() && args.LastLogIndex >= c.cfg.Log.LastIndex())
	canVote := c.votedFor == "" || c.votedFor == args.CandidateID

	if canVote && logOK {
		c.votedFor = args.CandidateID
		c.persistHardState()
		return &proto.RequestVoteReply{Term: c.currentTerm, VoteGranted: true}, true
	}
	return &proto.RequestVoteReply{Term: c.currentTerm, VoteGranted: false}, false
}

// handleAppendEntries implements the AppendEntries RPC receiver logic
// (Raft paper §5.3). Phase 2 has no log entries to check or apply yet —
// that arrives in Phase 3 — so this only implements the term/leader
// rules that election correctness depends on: rejecting stale leaders,
// stepping a candidate down when a legitimate current-term leader is
// heard from, and recognizing the sender as the current leader.
func (c *Core) handleAppendEntries(args *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, bool) {
	if args.Term > c.currentTerm {
		c.stepDown(args.Term)
	}
	if args.Term < c.currentTerm {
		return &proto.AppendEntriesReply{Term: c.currentTerm, Success: false}, false
	}

	if c.role == Candidate {
		c.role = Follower // paper §5.2: a candidate steps down on AppendEntries from a leader of >= its own term
	}
	c.leaderID = args.LeaderID
	return &proto.AppendEntriesReply{Term: c.currentTerm, Success: true}, true
}

// broadcastHeartbeats sends an empty AppendEntries (a heartbeat) to
// every peer. Only ever called while Leader.
func (c *Core) broadcastHeartbeats() {
	term := c.currentTerm // snapshot: safe to read here (Core's own goroutine), captured before spawning goroutines that must not touch c directly
	args := &proto.AppendEntriesArgs{
		Term:         term,
		LeaderID:     c.cfg.ID,
		PrevLogIndex: c.cfg.Log.LastIndex(),
		PrevLogTerm:  c.cfg.Log.LastTerm(),
	}
	for _, peer := range c.cfg.Peers {
		peer := peer
		go func() {
			reply, err := c.cfg.Transport.AppendEntries(peer, args)
			if err != nil {
				return
			}
			c.termUpdateCh <- reply.Term
		}()
	}
}

// persistHardState durably saves currentTerm/votedFor before any RPC
// reply that depends on them is sent — Raft paper §5: persistent state
// must hit stable storage before responding. A failure here means
// correctness can no longer be guaranteed, so it panics rather than
// silently continuing.
func (c *Core) persistHardState() {
	err := c.cfg.HardState.Save(storage.HardState{CurrentTerm: c.currentTerm, VotedFor: c.votedFor})
	if err != nil {
		panic(fmt.Sprintf("raft: failed to persist hard state: %v", err))
	}
}
