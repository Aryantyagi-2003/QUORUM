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
	Log       *storage.Log

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
	CommitIndex uint64
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

// appendEntriesResult carries an AppendEntries RPC's outcome back to
// Core's own goroutine. sentPrevLogIndex/sentEntryCount describe what
// was actually sent (captured at send time, not re-read from Core's
// current nextIndex, which may have moved on by the time this result
// arrives) so a successful reply can be turned into the correct
// matchIndex update.
type appendEntriesResult struct {
	peer             string
	term             uint64 // term this request was sent for
	sentPrevLogIndex uint64
	sentEntryCount   int
	reply            *proto.AppendEntriesReply // nil if err != nil
	err              error
}

type proposeRequest struct {
	command []byte
	reply   chan proposeReply
}

type proposeReply struct {
	index    uint64
	term     uint64
	isLeader bool
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

	// Leader-only replication state, initialized fresh in becomeLeader.
	// nextIndex/matchIndex follow the paper's Figure 2 definitions
	// exactly; peerBusy enforces at most one AppendEntries in flight per
	// peer at a time (see the Phase 3 design note in replicateToAllPeers
	// for why that gate exists instead of persistent per-peer
	// goroutines).
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	peerBusy   map[string]bool

	// commitIndex is tracked on every role (a follower advances it from
	// AppendEntries' LeaderCommit field; a leader advances it itself via
	// advanceCommitIndex). It is volatile, not persisted — per the
	// paper, a restarted node reconstructs it from scratch via
	// AppendEntries from whichever node is leader once it rejoins.
	commitIndex uint64

	voteReqCh   chan voteRequest
	appendReqCh chan appendRequest
	voteResCh   chan voteResult
	appendResCh chan appendEntriesResult
	proposeCh   chan proposeRequest
	stateCh     chan stateQuery
	stopCh      chan struct{}
	stopOnce    sync.Once

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
		cfg:         cfg,
		role:        Follower,
		currentTerm: hs.CurrentTerm,
		votedFor:    hs.VotedFor,
		voteReqCh:   make(chan voteRequest),
		appendReqCh: make(chan appendRequest),
		voteResCh:   make(chan voteResult),
		appendResCh: make(chan appendEntriesResult),
		proposeCh:   make(chan proposeRequest),
		stateCh:     make(chan stateQuery),
		stopCh:      make(chan struct{}),
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

		case <-electionTimer.C():
			c.startElection()
			electionTimer.Reset(c.randomizedElectionTimeout()) // a failed/split election must also time out and retry

		case res := <-c.voteResCh:
			if c.tallyVote(res) {
				electionTimer.Stop()
				heartbeatTicker = c.cfg.Clock.NewTicker(c.cfg.HeartbeatInterval)
				c.replicateToAllPeers() // send the first heartbeat/replication attempt immediately, don't wait a full interval
			}

		case <-tickerChan(heartbeatTicker):
			c.replicateToAllPeers()

		case res := <-c.appendResCh:
			if c.handleAppendEntriesResult(res) { // true exactly when this reply's term forced a step-down
				electionTimer.Reset(c.randomizedElectionTimeout())
			}

		case req := <-c.proposeCh:
			req.reply <- c.propose(req.command)

		case q := <-c.stateCh:
			q.reply <- c.snapshot()

		case <-c.stopCh:
			return
		}

		// Invariant, enforced unconditionally after every event rather
		// than ad hoc in each case above: a heartbeat ticker must never
		// keep running once this node isn't Leader. Any of the cases
		// above can step this node down (voteReqCh and appendReqCh via
		// handleRequestVote/handleAppendEntries seeing a higher term,
		// appendResCh via a stale reply's term, voteResCh via
		// tallyVote's own higher-term check) -- scattering this same
		// three-line cleanup across every one of those individual sites
		// is exactly what let one of them (voteReqCh) get missed the
		// first time this was written; a single check here can't be
		// missed by construction. Leaving a stopped leader's ticker
		// running would otherwise both misbehave (a non-leader still
		// trying to replicate) and, under FakeClock specifically,
		// deadlock a later AdvanceAndSync with nothing left to receive
		// its fire.
		if c.role != Leader && heartbeatTicker != nil {
			heartbeatTicker.Stop()
			heartbeatTicker = nil
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
	return Snapshot{
		Role: c.role, CurrentTerm: c.currentTerm, VotedFor: c.votedFor,
		LeaderID: c.leaderID, CommitIndex: c.commitIndex,
	}
}

// Propose implements RPCHandler-style channel access for client writes:
// it appends command to the leader's log and begins replicating it,
// returning immediately rather than waiting for the entry to commit.
// If this node isn't currently the leader, isLeader is false and
// index/term are meaningless — Phase 4's client-facing server is
// responsible for redirecting elsewhere in that case.
func (c *Core) Propose(command []byte) (index, term uint64, isLeader bool) {
	reply := make(chan proposeReply)
	select {
	case c.proposeCh <- proposeRequest{command: command, reply: reply}:
	case <-c.stopCh:
		return 0, 0, false
	}
	select {
	case r := <-reply:
		return r.index, r.term, r.isLeader
	case <-c.stopCh:
		return 0, 0, false
	}
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

	// Raft paper Figure 2: nextIndex initialized to leader's last log
	// index + 1 for every peer (optimistically assume they're fully
	// caught up; handleAppendEntriesResult backs this off on the first
	// rejection). matchIndex starts at 0 (nothing known replicated yet).
	lastIdx := c.cfg.Log.LastIndex()
	c.nextIndex = make(map[string]uint64, len(c.cfg.Peers))
	c.matchIndex = make(map[string]uint64, len(c.cfg.Peers))
	c.peerBusy = make(map[string]bool, len(c.cfg.Peers))
	for _, peer := range c.cfg.Peers {
		c.nextIndex[peer] = lastIdx + 1
		c.matchIndex[peer] = 0
		c.peerBusy[peer] = false
	}
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
// (Raft paper §5.3), including the log matching property and conflict
// resolution.
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

	// Log matching property (paper §5.3): reject unless we already have
	// an entry at PrevLogIndex with term PrevLogTerm. PrevLogIndex == 0
	// is the sentinel "start of log" and always trivially matches.
	//
	// The reply still resets the caller's election timer even on
	// rejection (returned true below in both reject branches) — a
	// follower mid-backtrack with a legitimate current-term leader
	// still shouldn't spuriously start its own election just because
	// this particular round's entries didn't line up yet.
	if args.PrevLogIndex > 0 {
		term, ok := c.cfg.Log.TermAt(args.PrevLogIndex)
		if !ok {
			return &proto.AppendEntriesReply{
				Term: c.currentTerm, Success: false,
				ConflictIndex: c.cfg.Log.LastIndex() + 1, ConflictTerm: 0,
			}, true
		}
		if term != args.PrevLogTerm {
			return &proto.AppendEntriesReply{
				Term: c.currentTerm, Success: false,
				ConflictIndex: c.firstIndexOfTerm(term), ConflictTerm: term,
			}, true
		}
	}

	// Log matching confirmed. Find the first entry (if any) that either
	// doesn't exist yet or conflicts with the leader's, truncate our
	// suffix from there (paper §5.3: "delete the existing entry and all
	// that follow it"), and append the rest.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		existingTerm, ok := c.cfg.Log.TermAt(idx)
		if ok && existingTerm == entry.Term {
			continue // already have this exact entry
		}
		if ok {
			if err := c.cfg.Log.TruncateFrom(idx); err != nil {
				panic(fmt.Sprintf("raft: failed to truncate conflicting log suffix: %v", err))
			}
		}
		if err := c.cfg.Log.Append(args.Entries[i:]); err != nil {
			panic(fmt.Sprintf("raft: failed to append replicated entries: %v", err))
		}
		break
	}

	if args.LeaderCommit > c.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNew {
			c.commitIndex = args.LeaderCommit
		} else {
			c.commitIndex = lastNew
		}
	}

	return &proto.AppendEntriesReply{Term: c.currentTerm, Success: true}, true
}

// firstIndexOfTerm returns the lowest log index holding term, for the
// AppendEntries conflict-reply fast-backtrack optimization (paper §5.3
// footnote). Callers only invoke this when TermAt has already confirmed
// term appears in the log, so the scan is guaranteed to find it.
func (c *Core) firstIndexOfTerm(term uint64) uint64 {
	for i := uint64(1); i <= c.cfg.Log.LastIndex(); i++ {
		if t, ok := c.cfg.Log.TermAt(i); ok && t == term {
			return i
		}
	}
	return 1
}

// lastIndexOfTerm returns the highest log index holding term, or
// (0, false) if the leader's own log has no entry from that term at
// all — used by handleAppendEntriesResult to decide how far to back up
// nextIndex on a rejection.
func (c *Core) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := c.cfg.Log.LastIndex(); i >= 1; i-- {
		if t, ok := c.cfg.Log.TermAt(i); ok && t == term {
			return i, true
		}
	}
	return 0, false
}

// replicateToAllPeers attempts to send each peer an AppendEntries built
// from that peer's current nextIndex — either real entries to replicate
// or, if the peer is already fully caught up, an empty one that serves
// as a heartbeat. Only ever called while Leader, on Core's own
// goroutine (heartbeat tick, right after becoming leader, and right
// after a successful Propose).
//
// peerBusy gates each peer to at most one outstanding AppendEntries at
// a time. Without it, a slow peer could accumulate multiple overlapping
// in-flight requests whose replies arrive out of order — processing a
// stale reply after a newer one already updated nextIndex/matchIndex
// would incorrectly regress them. This is deliberately simpler than
// giving each peer a persistent replicator goroutine with its own
// pipelining: a persistent goroutine would need the same ordering
// protection anyway, and single-outstanding-request is sufficient for
// this project's scope.
func (c *Core) replicateToAllPeers() {
	for _, peer := range c.cfg.Peers {
		if c.peerBusy[peer] {
			continue
		}
		c.peerBusy[peer] = true

		next := c.nextIndex[peer]
		prevIndex := next - 1
		prevTerm, _ := c.cfg.Log.TermAt(prevIndex) // (0, false) at prevIndex == 0 is the correct sentinel term
		entries := c.cfg.Log.Slice(next)
		args := &proto.AppendEntriesArgs{
			Term:         c.currentTerm,
			LeaderID:     c.cfg.ID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: c.commitIndex,
		}
		term := c.currentTerm
		sentEntryCount := len(entries)
		peer := peer
		go func() {
			reply, err := c.cfg.Transport.AppendEntries(peer, args)
			// Always report back, even on error — otherwise an
			// unreachable peer would stay gated busy forever and never
			// be retried on a later heartbeat tick.
			c.appendResCh <- appendEntriesResult{
				peer: peer, term: term, sentPrevLogIndex: prevIndex,
				sentEntryCount: sentEntryCount, reply: reply, err: err,
			}
		}()
	}
}

// handleAppendEntriesResult folds one AppendEntries reply into leader
// state: matchIndex/nextIndex on success, backtracking nextIndex via
// the conflict info on rejection, or stepping down on a higher term.
// Returns true exactly when this reply caused a step-down, so Run can
// react (reset the election timer, stop the heartbeat ticker) the same
// way it already does for tallyVote's analogous return value.
func (c *Core) handleAppendEntriesResult(res appendEntriesResult) bool {
	c.peerBusy[res.peer] = false // free the gate regardless of outcome

	if res.err != nil {
		return false // will retry on the next heartbeat tick
	}
	if res.reply.Term > c.currentTerm {
		c.stepDown(res.reply.Term)
		return true
	}
	if c.role != Leader || res.term != c.currentTerm {
		return false // no longer leader, or this reply is from an abandoned term
	}

	if res.reply.Success {
		newMatch := res.sentPrevLogIndex + uint64(res.sentEntryCount)
		if newMatch > c.matchIndex[res.peer] {
			c.matchIndex[res.peer] = newMatch
		}
		c.nextIndex[res.peer] = c.matchIndex[res.peer] + 1
		c.advanceCommitIndex()
		return false
	}

	// Rejected: back up nextIndex using the conflict info (paper §5.3
	// footnote fast-backtrack) rather than just decrementing by one.
	if res.reply.ConflictTerm != 0 {
		if idx, ok := c.lastIndexOfTerm(res.reply.ConflictTerm); ok {
			c.nextIndex[res.peer] = idx + 1
		} else {
			c.nextIndex[res.peer] = res.reply.ConflictIndex
		}
	} else {
		c.nextIndex[res.peer] = res.reply.ConflictIndex
	}
	if c.nextIndex[res.peer] < 1 {
		c.nextIndex[res.peer] = 1
	}
	return false
}

// advanceCommitIndex implements the paper's §5.4.2 rule: a leader may
// only conclude an entry is committed by counting replicas for entries
// from its OWN current term — never for an earlier-term entry, even if
// a majority already stores it. Handled deliberately here, not missed:
// committing an earlier-term entry via counting alone is exactly the
// scenario the paper's Figure 8 shows can be silently overwritten by a
// future leader, since winning an election only requires a log at
// least as up to date, not having replicated that specific entry. An
// earlier-term entry only becomes committed indirectly, as a side
// effect of a later current-term entry (at a higher index) reaching
// majority first.
func (c *Core) advanceCommitIndex() {
	for n := c.cfg.Log.LastIndex(); n > c.commitIndex; n-- {
		term, ok := c.cfg.Log.TermAt(n)
		if !ok {
			continue
		}
		if term != c.currentTerm {
			// Terms are non-decreasing with index in a valid leader log,
			// so everything below n is from an equally-old or older term
			// too — none of it is countable either. Stop scanning.
			break
		}
		count := 1 // self
		for _, peer := range c.cfg.Peers {
			if c.matchIndex[peer] >= n {
				count++
			}
		}
		if count*2 > len(c.cfg.Peers)+1 {
			c.commitIndex = n
			break
		}
	}
}

// propose implements Propose's logic on Core's own goroutine.
func (c *Core) propose(command []byte) proposeReply {
	if c.role != Leader {
		return proposeReply{isLeader: false}
	}
	index := c.cfg.Log.LastIndex() + 1
	entry := proto.LogEntry{Term: c.currentTerm, Index: index, Command: command}
	if err := c.cfg.Log.Append([]proto.LogEntry{entry}); err != nil {
		panic(fmt.Sprintf("raft: failed to append proposed entry: %v", err))
	}
	if len(c.cfg.Peers) == 0 {
		// A single-node cluster commits immediately -- there's no one
		// else to wait on for a majority.
		c.commitIndex = index
	}
	c.replicateToAllPeers()
	return proposeReply{index: index, term: c.currentTerm, isLeader: true}
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
