package raft

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// logsEqual compares two nodes' logs entry-by-entry (term and command
// bytes; index is implied by position). Used to confirm replication
// actually converges a follower's log to the leader's, not just that
// commitIndex looks right.
func logsEqual(t *testing.T, a, b *Core) bool {
	t.Helper()
	if a.cfg.Log.LastIndex() != b.cfg.Log.LastIndex() {
		return false
	}
	for i := uint64(1); i <= a.cfg.Log.LastIndex(); i++ {
		ea, _ := a.cfg.Log.Get(i)
		eb, _ := b.cfg.Log.Get(i)
		if ea.Term != eb.Term || !bytes.Equal(ea.Command, eb.Command) {
			return false
		}
	}
	return true
}

// TestReplication_BasicMajorityCommit proposes several entries on a
// naturally-elected leader and confirms: commitIndex advances to cover
// all of them, and every follower's log ends up byte-identical to the
// leader's -- not just "commitIndex looks right" but the actual
// replicated content matches.
func TestReplication_BasicMajorityCommit(t *testing.T) {
	opts := defaultClusterOpts()
	tc := newTestCluster(t, 3, opts)

	if !runElectionUntilConverged(t, tc, 2*opts.electionTimeoutMax, 5*time.Millisecond) {
		t.Fatal("initial election did not converge")
	}
	leaderID := tc.leaders()[0]
	leader := tc.cores[leaderID]

	commands := [][]byte{[]byte("set x 1"), []byte("set y 2"), []byte("delete x")}
	var lastIndex uint64
	for _, cmd := range commands {
		idx, _, isLeader := leader.Propose(cmd)
		if !isLeader {
			t.Fatalf("Propose(%q): leader %s reported isLeader=false", cmd, leaderID)
		}
		lastIndex = idx
	}
	if lastIndex != uint64(len(commands)) {
		t.Fatalf("last proposed index = %d, want %d", lastIndex, len(commands))
	}

	committed := advanceUntilSettled(t, tc.clock, 3*time.Second, opts.heartbeatInterval, 200*time.Millisecond, func() bool {
		return leader.State().CommitIndex == lastIndex
	})
	if !committed {
		t.Fatalf("leader's commitIndex = %d, want %d", leader.State().CommitIndex, lastIndex)
	}

	for _, id := range tc.ids {
		if id == leaderID {
			continue
		}
		caughtUp := advanceUntilSettled(t, tc.clock, 3*time.Second, opts.heartbeatInterval, 200*time.Millisecond, func() bool {
			return logsEqual(t, leader, tc.cores[id])
		})
		if !caughtUp {
			t.Fatalf("follower %s's log never matched the leader's", id)
		}
	}
}

// TestReplication_LaggingFollowerCatchesUpViaBacktrack proves the
// AppendEntries-rejection backtrack path (paper §5.3 footnote): a
// follower that's badly behind (here, completely empty) gets a leader
// whose optimistic nextIndex assumption is wrong, is rejected on the
// first attempt, and correctly backs all the way down to the start of
// the log before succeeding and catching up completely.
//
// The mechanism: nodeC is isolated from the very first leader before
// any entries exist, so it never learns of them (followers only ever
// learn of entries via AppendEntries from a leader -- there's no
// gossip between followers). When that leader is later replaced, the
// new leader is guaranteed (by the log-up-to-date voting rule, not by
// luck) to be whichever node already has the fullest log -- nodeC's
// empty log can never look "at least as up to date," so it structurally
// cannot win that election. That new leader then optimistically
// initializes nodeC's nextIndex to its own log's end, gets rejected,
// and backs off to the very start.
func TestReplication_LaggingFollowerCatchesUpViaBacktrack(t *testing.T) {
	opts := defaultClusterOpts()
	tc := newTestCluster(t, 3, opts)

	if !runElectionUntilConverged(t, tc, 2*opts.electionTimeoutMax, 5*time.Millisecond) {
		t.Fatal("initial election did not converge")
	}
	firstLeaderID := tc.leaders()[0]
	firstLeader := tc.cores[firstLeaderID]

	var isolated string
	for _, id := range tc.ids {
		if id != firstLeaderID {
			isolated = id
			break
		}
	}
	tc.net.Drop(firstLeaderID, isolated)
	tc.net.Drop(isolated, firstLeaderID)

	commands := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	var lastIndex uint64
	for _, cmd := range commands {
		idx, _, isLeader := firstLeader.Propose(cmd)
		if !isLeader {
			t.Fatalf("Propose(%q): not leader", cmd)
		}
		lastIndex = idx
	}

	committed := advanceUntilSettled(t, tc.clock, 3*time.Second, opts.heartbeatInterval, 200*time.Millisecond, func() bool {
		return firstLeader.State().CommitIndex == lastIndex
	})
	if !committed {
		t.Fatalf("first leader's commitIndex = %d, want %d (majority excluding the isolated node should suffice)",
			firstLeader.State().CommitIndex, lastIndex)
	}
	if got, ok := tc.cores[isolated].cfg.Log.Get(1); ok {
		t.Fatalf("isolated node should have no entries yet, got %+v", got)
	}

	firstLeader.Stop()
	tc.net.Unregister(firstLeaderID)

	var newLeaderID string
	newLeaderElected := advanceUntilSettled(t, tc.clock, 2*opts.electionTimeoutMax, 5*time.Millisecond, 200*time.Millisecond, func() bool {
		leaders := tc.leaders()
		if len(leaders) == 1 && leaders[0] != firstLeaderID {
			newLeaderID = leaders[0]
			return true
		}
		return false
	})
	if !newLeaderElected {
		t.Fatal("no new leader elected after the first leader stopped")
	}
	if newLeaderID == isolated {
		t.Fatalf("the previously-isolated, log-empty node %s won the election -- log-up-to-date safety violation", isolated)
	}

	newLeader := tc.cores[newLeaderID]
	caughtUp := advanceUntilSettled(t, tc.clock, 3*time.Second, opts.heartbeatInterval, 200*time.Millisecond, func() bool {
		return logsEqual(t, newLeader, tc.cores[isolated])
	})
	if !caughtUp {
		t.Fatalf("previously-isolated node %s never caught up to the new leader %s", isolated, newLeaderID)
	}
	if got := tc.cores[isolated].cfg.Log.LastIndex(); got != lastIndex {
		t.Fatalf("caught-up node's LastIndex = %d, want %d", got, lastIndex)
	}
}

// TestReplication_ConflictingUncommittedSuffixOverwritten forces a
// genuine log conflict -- not a length gap like the backtrack test
// above, but a real term mismatch -- and confirms the follower's
// conflicting, never-committed entry is discarded and replaced with the
// new leader's entry at the same index, per the log matching property
// (paper §5.3: "delete the existing entry and all that follow it").
//
// Constructed via network topology plus fully deterministic, staggered
// FIXED election timeouts for every node (the same determinism
// technique as Phase 2's split-vote test) -- not the default seeded-but-
// randomized timeouts newTestCluster's helpers use elsewhere, and
// deliberately so: with randomized timeouts, a loser of the second
// election can occasionally re-time-out and start a spurious extra
// candidacy (a real, harmless Raft behavior, but one that adds
// unnecessary election churn -- and every term change costs a real
// fsync via persistHardState, so uncontrolled extra churn here just
// means more real disk I/O than the scenario actually needs). Staggered
// fixed timeouts make each of the two elections in this test resolve in
// exactly one round, deterministically, with no possibility of a third
// party jumping in.
//
// nodeA is isolated the instant it becomes leader, so its proposed
// entry X can never leave its own log. nodeB and nodeC, both still on
// empty logs, elect nodeB specifically (shorter timeout) and commit a
// different entry Y at the same index. Healing the network and letting
// nodeA rejoin exercises the ConflictTerm-set branch of the backtrack
// logic specifically (nodeA has *an* entry at the conflict point, just
// the wrong one), which the plain-backtrack test above doesn't reach.
func TestReplication_ConflictingUncommittedSuffixOverwritten(t *testing.T) {
	net := NewFakeNetwork()
	clock := NewFakeClock(time.Unix(0, 0))
	ids := []string{"nodeA", "nodeB", "nodeC"}
	cores := make(map[string]*Core)

	newNode := func(id string, timeout time.Duration) *Core {
		l, err := storage.OpenLog(t.TempDir())
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		t.Cleanup(func() { l.Close() })
		c := NewCore(Config{
			ID:                 id,
			Peers:              excludeID(ids, id),
			Transport:          &FakeTransport{Self: id, Network: net},
			Clock:              clock,
			Rand:               rand.New(rand.NewSource(1)),
			HardState:          storage.NewHardStateStore(t.TempDir()),
			Log:                l,
			ElectionTimeoutMin: timeout,
			ElectionTimeoutMax: timeout,
			HeartbeatInterval:  10 * time.Millisecond,
		})
		net.Register(id, c)
		cores[id] = c
		go c.Run()
		t.Cleanup(c.Stop)
		return c
	}

	// nodeA wins the first election (shortest timeout). nodeB wins the
	// second, once nodeA is isolated (next-shortest). nodeC's timeout is
	// far outside this test's virtual-time budget, so it never
	// spontaneously starts a competing candidacy.
	newNode("nodeA", 50*time.Millisecond)
	newNode("nodeB", 100*time.Millisecond)
	newNode("nodeC", 5*time.Second)

	firstLeaderElected := advanceUntilSettled(t, clock, 3*time.Second, 5*time.Millisecond, 200*time.Millisecond, func() bool {
		return cores["nodeA"].State().Role == Leader
	})
	if !firstLeaderElected {
		t.Fatal("nodeA never became leader")
	}

	// Isolate nodeA completely before it can replicate anything.
	net.Drop("nodeA", "nodeB")
	net.Drop("nodeB", "nodeA")
	net.Drop("nodeA", "nodeC")
	net.Drop("nodeC", "nodeA")

	idxX, termX, isLeader := cores["nodeA"].Propose([]byte("X"))
	if !isLeader {
		t.Fatal("nodeA: Propose failed, not leader")
	}
	if cores["nodeA"].State().CommitIndex != 0 {
		t.Fatal("nodeA's isolated, unreplicated entry must not be committed")
	}

	secondLeaderElected := advanceUntilSettled(t, clock, 3*time.Second, 5*time.Millisecond, 200*time.Millisecond, func() bool {
		return cores["nodeB"].State().Role == Leader
	})
	if !secondLeaderElected {
		t.Fatalf("nodeB never became leader while nodeA was isolated: nodeA=%+v nodeB=%+v nodeC=%+v",
			cores["nodeA"].State(), cores["nodeB"].State(), cores["nodeC"].State())
	}

	idxY, termY, isLeader := cores["nodeB"].Propose([]byte("Y"))
	if !isLeader {
		t.Fatal("nodeB: Propose failed, not leader")
	}
	if idxY != idxX {
		t.Fatalf("Y's index = %d, want %d (same index as X, for a genuine conflict)", idxY, idxX)
	}
	if termY == termX {
		t.Fatalf("Y's term = %d, want it to differ from X's term %d", termY, termX)
	}

	committed := advanceUntilSettled(t, clock, 3*time.Second, 10*time.Millisecond, 200*time.Millisecond, func() bool {
		return cores["nodeB"].State().CommitIndex == idxY
	})
	if !committed {
		t.Fatalf("nodeB's commitIndex = %d, want %d", cores["nodeB"].State().CommitIndex, idxY)
	}

	// Heal the network and let nodeA rejoin. Its stale, uncommitted X
	// must be discarded and replaced with Y.
	net.HealAll()
	resolved := advanceUntilSettled(t, clock, 3*time.Second, 10*time.Millisecond, 200*time.Millisecond, func() bool {
		e, ok := cores["nodeA"].cfg.Log.Get(idxX)
		return ok && e.Term == termY && bytes.Equal(e.Command, []byte("Y"))
	})
	if !resolved {
		e, _ := cores["nodeA"].cfg.Log.Get(idxX)
		t.Fatalf("nodeA's entry at index %d never converged to Y: got %+v", idxX, e)
	}
}

// TestReplication_Figure8_DoesNotCommitPriorTermEntryByCountingAlone is
// the single most important replication test: a direct, faithfully
// constructed reproduction of the Raft paper's Figure 8 danger. It does
// not skip or simplify the safety property under test -- only an
// incidental detail (which specific log index) differs from the paper's
// illustration, noted below.
//
// Construction: nodeA is elected leader (term 1) and proposes entry E,
// but is isolated before E can reach anyone else -- E exists only in
// nodeA's own log. nodeA then genuinely crashes and restarts (Stop,
// then a fresh Core opened over the SAME on-disk log/hardstate
// directories, exactly like Phase 1's kill-9 durability test) --
// restarting always begins as Follower per the paper, so nodeA must
// win a brand new election to lead again, which increments its term
// past 1. Because nodeB/nodeC still have completely empty logs at this
// point, nodeA's single-entry log is trivially "at least as up to
// date," so it wins cleanly.
//
// Now leading in term 2 with E (term 1) still the only entry in its
// log, nodeA replicates to nodeB. That reaches a majority (nodeA +
// nodeB = 2 of 3) -- but E is from term 1, not nodeA's current term 2.
// This is exactly the dangerous precondition: a majority holds an
// entry from an older term, under a leader who hasn't yet replicated
// anything from its own current term. The assertion that matters most
// in this whole test suite: commitIndex must NOT advance here.
//
// The test then completes the paper's §5.4.2 rule by showing the other
// half: once nodeA proposes and replicates a NEW entry from its own
// term 2 to the same majority, commitIndex advances past that new
// entry -- and, as a single watermark, that indirectly and correctly
// commits E too. (What could go wrong if the direct-counting rule were
// absent -- E being silently overwritten by a rival leader after being
// wrongly marked committed -- is exactly the mechanism proven
// separately by TestReplication_ConflictingUncommittedSuffixOverwritten
// above: an uncommitted entry at a shared index is discarded, which
// would be a lost-committed-entry safety violation if E had already
// been (wrongly) committed at that point.)
func TestReplication_Figure8_DoesNotCommitPriorTermEntryByCountingAlone(t *testing.T) {
	net := NewFakeNetwork()
	clock := NewFakeClock(time.Unix(0, 0))
	ids := []string{"nodeA", "nodeB", "nodeC"}

	type nodeHandle struct {
		dataDir string
		core    *Core
	}
	nodes := make(map[string]*nodeHandle)

	// Each node needs its own distinct Rand seed -- sharing one seed
	// across nodes with real timeout jitter (unlike the fixed-timeout
	// tests elsewhere in this file) makes every node draw the identical
	// jitter sequence, which can produce a deterministic, unbreakable
	// repeating tie rather than the realistic independent randomization
	// Raft relies on to resolve split votes.
	seedFor := func(id string) int64 {
		for i, x := range ids {
			if x == id {
				return int64(i + 1)
			}
		}
		return 99
	}

	newNode := func(id string, reuseDataDir string) *Core {
		dataDir := reuseDataDir
		if dataDir == "" {
			dataDir = t.TempDir()
		}
		l, err := storage.OpenLog(dataDir)
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		t.Cleanup(func() { l.Close() })
		c := NewCore(Config{
			ID:                 id,
			Peers:              excludeID(ids, id),
			Transport:          &FakeTransport{Self: id, Network: net},
			Clock:              clock,
			Rand:               rand.New(rand.NewSource(seedFor(id))),
			HardState:          storage.NewHardStateStore(dataDir),
			Log:                l,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  20 * time.Millisecond,
		})
		net.Register(id, c)
		nodes[id] = &nodeHandle{dataDir: dataDir, core: c}
		go c.Run()
		return c
	}

	for _, id := range ids {
		c := newNode(id, "")
		t.Cleanup(c.Stop)
	}

	electLeader := func(virtualBudget time.Duration) string {
		t.Helper()
		var leaderID string
		ok := advanceUntilSettled(t, clock, virtualBudget, 5*time.Millisecond, 200*time.Millisecond, func() bool {
			var leaders []string
			for _, id := range ids {
				if nodes[id].core.State().Role == Leader {
					leaders = append(leaders, id)
				}
			}
			if len(leaders) == 1 {
				leaderID = leaders[0]
				return true
			}
			return false
		})
		if !ok {
			t.Fatal("election did not converge")
		}
		return leaderID
	}

	// Step 1: elect an initial leader (should be nodeA, since it's the
	// only one seeded to win deterministically -- but we don't assume
	// that; we isolate whoever it actually is).
	firstLeaderID := electLeader(2 * 300 * time.Millisecond)
	firstLeader := nodes[firstLeaderID].core

	// Isolate the leader completely before it can replicate anything.
	for _, id := range ids {
		if id == firstLeaderID {
			continue
		}
		net.Drop(firstLeaderID, id)
		net.Drop(id, firstLeaderID)
	}

	idxE, termE, isLeader := firstLeader.Propose([]byte("E"))
	if !isLeader {
		t.Fatal("Propose failed, not leader")
	}
	if firstLeader.State().CommitIndex != 0 {
		t.Fatal("E must not be committed: it never left the isolated leader's own log")
	}

	// Step 2: the isolated leader genuinely crashes and restarts --
	// Stop, then a fresh Core over the SAME on-disk storage (log +
	// hardstate survive, exactly like a real process restart).
	dataDir := nodes[firstLeaderID].dataDir
	firstLeader.Stop()
	net.Unregister(firstLeaderID)
	restarted := newNode(firstLeaderID, dataDir)
	t.Cleanup(restarted.Stop)

	if got := restarted.cfg.Log.LastIndex(); got != idxE {
		t.Fatalf("restarted node's log did not survive: LastIndex = %d, want %d", got, idxE)
	}
	if got := restarted.State().CurrentTerm; got != termE {
		t.Fatalf("restarted node's persisted term = %d, want %d (must not forget it across restart)", got, termE)
	}

	// Heal the network so the restarted node can be reached again. The
	// log-up-to-date rule guarantees the OTHER two nodes can never
	// grant a vote to each other over the restarted node (its log,
	// with E, is strictly more up to date than their empty ones) --
	// but it does NOT prevent them from electing one of themselves
	// using only each other's votes, entirely bypassing the restarted
	// node, since 2 of 3 is already a majority without it. To make the
	// restarted node's win deterministic rather than a timing race,
	// also cut the link directly between those other two, so the only
	// election that can possibly succeed is one the restarted node
	// wins (either as candidate, since both others must grant it being
	// at least as up to date, or not at all).
	net.HealAll()
	var others []string
	for _, id := range ids {
		if id != firstLeaderID {
			others = append(others, id)
		}
	}
	net.Drop(others[0], others[1])
	net.Drop(others[1], others[0])

	newLeaderID := electLeader(2 * 300 * time.Millisecond)
	if newLeaderID != firstLeaderID {
		t.Fatalf("expected the restarted node (%s) to win re-election on the strength of its more up-to-date log, but %s won instead",
			firstLeaderID, newLeaderID)
	}
	newLeaderTerm := restarted.State().CurrentTerm
	if newLeaderTerm <= termE {
		t.Fatalf("re-elected leader's term = %d, want > %d (E's original term)", newLeaderTerm, termE)
	}

	// Step 3: THE critical moment. Wait for E to reach a majority
	// (restarted + whichever of nodeB/nodeC it replicates to first) --
	// but assert commitIndex stays at 0 throughout, since E is from an
	// earlier term than the leader's current one.
	reachedMajorityWithoutCommitting := advanceUntilSettled(t, clock, 3*time.Second, 20*time.Millisecond, 200*time.Millisecond, func() bool {
		count := 1 // self
		for _, id := range ids {
			if id == firstLeaderID {
				continue
			}
			if e, ok := nodes[id].core.cfg.Log.Get(idxE); ok && e.Term == termE {
				count++
			}
		}
		if restarted.State().CommitIndex != 0 {
			t.Fatalf("commitIndex advanced to %d based on a prior-term entry replicated by counting alone -- §5.4.2 safety violation", restarted.State().CommitIndex)
		}
		return count*2 > len(ids) // majority reached
	})
	if !reachedMajorityWithoutCommitting {
		t.Fatal("E never reached a majority under the new leader's term -- test setup did not exercise the intended scenario")
	}
	if restarted.State().CommitIndex != 0 {
		t.Fatalf("commitIndex = %d after E reached majority via an old-term entry, want 0 (must not commit by counting alone)", restarted.State().CommitIndex)
	}

	// Step 4: complete the rule's other half. A NEW entry from the
	// leader's OWN current term, once it reaches the same majority,
	// commits -- and, as a single watermark, that correctly and
	// indirectly commits E too.
	idxF, termF, isLeader := restarted.Propose([]byte("F"))
	if !isLeader {
		t.Fatal("Propose failed, not leader")
	}
	if termF != newLeaderTerm {
		t.Fatalf("F's term = %d, want %d (the leader's own current term)", termF, newLeaderTerm)
	}

	fCommitted := advanceUntilSettled(t, clock, 3*time.Second, 20*time.Millisecond, 200*time.Millisecond, func() bool {
		return restarted.State().CommitIndex == idxF
	})
	if !fCommitted {
		t.Fatalf("commitIndex = %d, want %d (F, from the leader's own term, should commit once it reaches majority)",
			restarted.State().CommitIndex, idxF)
	}
	// E, one index below F, must now be indirectly committed too --
	// commitIndex is a single watermark, not per-entry bookkeeping.
	if idxE > restarted.State().CommitIndex {
		t.Fatalf("E at index %d is not covered by commitIndex %d -- it should be indirectly committed now that F committed above it",
			idxE, restarted.State().CommitIndex)
	}
}
