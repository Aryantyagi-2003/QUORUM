package raft

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

func newBareCore(t *testing.T, id string, peers []string, net *FakeNetwork, clock Clock, minTimeout, maxTimeout time.Duration) *Core {
	t.Helper()
	l, err := storage.OpenLog(t.TempDir())
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	c := NewCore(Config{
		ID:                 id,
		Peers:              peers,
		Transport:          &FakeTransport{Self: id, Network: net},
		Clock:              clock,
		Rand:               rand.New(rand.NewSource(1)),
		HardState:          storage.NewHardStateStore(t.TempDir()),
		Log:                l,
		ElectionTimeoutMin: minTimeout,
		ElectionTimeoutMax: maxTimeout,
		HeartbeatInterval:  minTimeout / 10,
	})
	return c
}

// TestElectionTimer_ExactBoundary proves the election timeout fires at
// exactly the configured duration, not "eventually" — one simulated
// tick before the deadline, no election has started; advancing exactly
// to the deadline starts one. This is only possible to assert this
// precisely because the clock is virtual: a real-timer version of this
// test could only ever claim "usually converges within some generous
// margin."
func TestElectionTimer_ExactBoundary(t *testing.T) {
	net := NewFakeNetwork()
	clock := NewFakeClock(time.Unix(0, 0))
	const timeout = 150 * time.Millisecond

	// One peer that is never registered with the network, so the
	// RequestVote this node fans out on timeout can never be answered —
	// the node is left observably in Candidate state (not fast-forwarded
	// to Leader), which keeps this test purely about the timer boundary.
	c := newBareCore(t, "node0", []string{"node1"}, net, clock, timeout, timeout)
	net.Register("node0", c)
	go c.Run()
	t.Cleanup(c.Stop)

	clock.AdvanceAndSync(timeout - time.Millisecond)
	if got := c.State(); got.Role != Follower || got.CurrentTerm != 0 {
		t.Fatalf("one tick before timeout: got %+v, want Follower term 0 (no election yet)", got)
	}

	clock.AdvanceAndSync(time.Millisecond) // now exactly at `timeout`
	if got := c.State(); got.Role != Candidate || got.CurrentTerm != 1 {
		t.Fatalf("exactly at timeout: got %+v, want Candidate term 1 (election started)", got)
	}
}

// TestElectionTimer_DoesNotFireBeforeBoundaryOnRepeatedAdvances is the
// same property checked incrementally, to make sure no fractional
// advance anywhere along the way accidentally crosses the boundary
// early.
func TestElectionTimer_DoesNotFireBeforeBoundaryOnRepeatedAdvances(t *testing.T) {
	net := NewFakeNetwork()
	clock := NewFakeClock(time.Unix(0, 0))
	const timeout = 150 * time.Millisecond

	c := newBareCore(t, "node0", []string{"node1"}, net, clock, timeout, timeout)
	net.Register("node0", c)
	go c.Run()
	t.Cleanup(c.Stop)

	for i := 0; i < 149; i++ {
		clock.AdvanceAndSync(time.Millisecond)
		if got := c.State(); got.Role != Follower {
			t.Fatalf("after %dms: got role %v, want Follower (timeout is %v)", i+1, got.Role, timeout)
		}
	}
	clock.AdvanceAndSync(time.Millisecond) // 150th ms
	if got := c.State(); got.Role != Candidate {
		t.Fatalf("after 150ms: got role %v, want Candidate", got.Role)
	}
}

func TestSingleNodeCluster_BecomesLeaderImmediatelyOnTimeout(t *testing.T) {
	net := NewFakeNetwork()
	clock := NewFakeClock(time.Unix(0, 0))
	const timeout = 150 * time.Millisecond

	c := newBareCore(t, "node0", nil, net, clock, timeout, timeout)
	net.Register("node0", c)
	go c.Run()
	t.Cleanup(c.Stop)

	clock.AdvanceAndSync(timeout)
	if got := c.State(); got.Role != Leader || got.CurrentTerm != 1 {
		t.Fatalf("got %+v, want Leader term 1 (a lone node wins its own election)", got)
	}
}

// The remaining tests below call the RPC-handling logic directly,
// without Run(), Clock, or Transport involved at all — pure
// state-transition tests of the vote-granting/denial and term rules,
// same style called for by the original quality bar ("election timeout
// firing, vote granting/denial logic ... testable without actually
// spinning up real network connections").

func newHandlerOnlyCore(t *testing.T, id string, currentTerm uint64, votedFor string) *Core {
	t.Helper()
	l, err := storage.OpenLog(t.TempDir())
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	hs := storage.NewHardStateStore(t.TempDir())
	if err := hs.Save(storage.HardState{CurrentTerm: currentTerm, VotedFor: votedFor}); err != nil {
		t.Fatalf("Save hardstate: %v", err)
	}
	return NewCore(Config{
		ID:                 id,
		HardState:          hs,
		Log:                l,
		Clock:              NewFakeClock(time.Unix(0, 0)),
		Rand:               rand.New(rand.NewSource(1)),
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 150 * time.Millisecond,
	})
}

func TestHandleRequestVote_GrantsWhenNoVoteCastYet(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "")
	reply, resetTimer := c.handleRequestVote(&proto.RequestVoteArgs{
		Term: 5, CandidateID: "node1", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !reply.VoteGranted || !resetTimer {
		t.Fatalf("got granted=%v resetTimer=%v, want both true", reply.VoteGranted, resetTimer)
	}
	if c.votedFor != "node1" {
		t.Fatalf("votedFor = %q, want node1", c.votedFor)
	}
}

func TestHandleRequestVote_DeniesSecondVoteInSameTerm(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "node1") // already voted for node1 this term
	reply, resetTimer := c.handleRequestVote(&proto.RequestVoteArgs{
		Term: 5, CandidateID: "node2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if reply.VoteGranted || resetTimer {
		t.Fatalf("got granted=%v resetTimer=%v, want both false (already voted for a different candidate)", reply.VoteGranted, resetTimer)
	}
}

func TestHandleRequestVote_GrantsRepeatToSameCandidateSameTerm(t *testing.T) {
	// A duplicate/retried RequestVote from the SAME candidate we already
	// voted for this term must still be granted (idempotent), per the
	// paper's vote-granting rule being about the candidate identity, not
	// "have I replied before."
	c := newHandlerOnlyCore(t, "node0", 5, "node1")
	reply, _ := c.handleRequestVote(&proto.RequestVoteArgs{
		Term: 5, CandidateID: "node1", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !reply.VoteGranted {
		t.Fatal("expected vote granted again for the same already-voted-for candidate")
	}
}

func TestHandleRequestVote_RejectsStaleTerm(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "")
	reply, resetTimer := c.handleRequestVote(&proto.RequestVoteArgs{
		Term: 3, CandidateID: "node1", LastLogIndex: 0, LastLogTerm: 0,
	})
	if reply.VoteGranted || resetTimer || reply.Term != 5 {
		t.Fatalf("got %+v resetTimer=%v, want denied, resetTimer=false, term echoed back as 5", reply, resetTimer)
	}
}

func TestHandleRequestVote_HigherTermCandidateWithStaleLogIsRejected(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "")
	// Give node0 a log entry so it has a non-zero LastLogTerm/LastLogIndex.
	if err := c.cfg.Log.Append([]proto.LogEntry{{Term: 5, Index: 1, Command: []byte("x")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	reply, _ := c.handleRequestVote(&proto.RequestVoteArgs{
		Term: 6, CandidateID: "node1", LastLogIndex: 0, LastLogTerm: 0, // candidate's log is behind ours
	})
	if reply.VoteGranted {
		t.Fatal("expected vote denied: candidate's log is less up-to-date than ours")
	}
	// But our term must still have advanced to the candidate's higher term.
	if c.currentTerm != 6 {
		t.Fatalf("currentTerm = %d, want 6 (term always advances even if the vote itself is denied)", c.currentTerm)
	}
}

func TestHandleAppendEntries_HigherTermStepsDownLeader(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "node0")
	c.role = Leader
	reply, resetTimer := c.handleAppendEntries(&proto.AppendEntriesArgs{Term: 6, LeaderID: "node1"})
	if !reply.Success || !resetTimer {
		t.Fatalf("got success=%v resetTimer=%v, want both true", reply.Success, resetTimer)
	}
	if c.role != Follower {
		t.Fatalf("role = %v, want Follower (must step down on higher term)", c.role)
	}
	if c.currentTerm != 6 {
		t.Fatalf("currentTerm = %d, want 6", c.currentTerm)
	}
	if c.votedFor != "" {
		t.Fatalf("votedFor = %q, want cleared on term advance", c.votedFor)
	}
	if c.leaderID != "node1" {
		t.Fatalf("leaderID = %q, want node1", c.leaderID)
	}
}

func TestHandleAppendEntries_CandidateStepsDownOnCurrentTermLeader(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "node0")
	c.role = Candidate
	reply, resetTimer := c.handleAppendEntries(&proto.AppendEntriesArgs{Term: 5, LeaderID: "node1"})
	if !reply.Success || !resetTimer {
		t.Fatalf("got success=%v resetTimer=%v, want both true", reply.Success, resetTimer)
	}
	if c.role != Follower {
		t.Fatalf("role = %v, want Follower", c.role)
	}
}

func TestHandleAppendEntries_RejectsStaleLeaderTerm(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "")
	reply, resetTimer := c.handleAppendEntries(&proto.AppendEntriesArgs{Term: 3, LeaderID: "node1"})
	if reply.Success || resetTimer || reply.Term != 5 {
		t.Fatalf("got %+v resetTimer=%v, want rejected, resetTimer=false, term echoed as 5", reply, resetTimer)
	}
}

func TestTallyVote_HigherPeerTermStepsDownCandidate(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "node0")
	c.role = Candidate
	c.currentTerm = 5
	becameLeader := c.tallyVote(voteResult{term: 5, granted: false, peerTerm: 9})
	if becameLeader {
		t.Fatal("must not become leader")
	}
	if c.role != Follower || c.currentTerm != 9 {
		t.Fatalf("got role=%v term=%d, want Follower term 9", c.role, c.currentTerm)
	}
}

func TestTallyVote_StaleReplyFromAbandonedElectionIsIgnored(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 5, "node0")
	c.role = Candidate
	c.currentTerm = 6 // node has since moved on to a later election
	c.votesReceived = 1
	becameLeader := c.tallyVote(voteResult{term: 5, granted: true, peerTerm: 5}) // reply for the OLD term-5 election
	if becameLeader {
		t.Fatal("a stale reply must never cause a leader transition")
	}
	if c.votesReceived != 1 {
		t.Fatalf("votesReceived = %d, want unchanged at 1", c.votesReceived)
	}
}

func TestTallyVote_MajorityElectsLeader(t *testing.T) {
	c := newHandlerOnlyCore(t, "node0", 1, "node0")
	c.role = Candidate
	c.currentTerm = 1
	c.cfg.Peers = []string{"node1", "node2", "node3", "node4"} // 5-node cluster, need 3 votes total
	c.votesReceived = 1                                        // self-vote

	if c.tallyVote(voteResult{term: 1, granted: true, peerTerm: 1}) {
		t.Fatal("2 of 5 votes must not yet elect a leader")
	}
	if !c.tallyVote(voteResult{term: 1, granted: true, peerTerm: 1}) {
		t.Fatal("3 of 5 votes must elect a leader")
	}
	if c.role != Leader {
		t.Fatalf("role = %v, want Leader", c.role)
	}
}
