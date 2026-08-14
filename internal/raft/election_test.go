package raft

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// waitForCondition polls cond on a real (very short) interval up to a
// generous real-time budget. This is NOT a workaround to the FakeClock
// discipline used everywhere else in this package — Raft's actual
// timing decisions (when an election starts, when it should have
// converged relative to virtual time) are still driven exclusively by
// clock.AdvanceAndSync, deterministically. What this helper waits on is
// a completely different concern: after AdvanceAndSync fires a timer,
// Core's reaction (spawning RequestVote goroutines, the FakeTransport
// call chain, tallying the replies) runs on real goroutines that the
// Go scheduler, not virtual time, decides when to run. AdvanceAndSync
// is deliberately scoped to not wait for that cascade (see its doc
// comment) — so the test does, explicitly, here. Since every step in
// that cascade is an in-process function call with no real I/O, it
// normally completes in microseconds, so the generous budget below is
// not "hoping timing works out" in the way a real-timer election test
// would be — it's waiting out ordinary goroutine scheduling.
func waitForCondition(t *testing.T, budget time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// advanceUntilSettled advances clock in a sequence of steps, up to
// virtualBudget of total virtual time, and after EACH step pauses
// (waitForCondition, not a further clock advance) for up to
// settleBudget of real time to let that step's async, goroutine-driven
// reactions finish before advancing again.
//
// This two-level shape — advance once, then only poll (never advance)
// while waiting to settle — is required, not just a style preference:
// AdvanceAndSync deliberately does not wait for Core's reaction to a
// fire (see its doc comment), so a loop that calls it repeatedly with
// no pause in between can advance virtual time faster than goroutine
// scheduling can keep up. Concretely, that lets a node's OWN election
// timer fire a second time (a real re-timeout, correctly incrementing
// its term again) before the first round's RequestVote replies have
// even been tallied, cascading into spurious extra elections that have
// nothing to do with the scenario under test. Pausing to settle after
// every single step keeps virtual time from ever getting more than one
// step ahead of what's actually been processed.
func advanceUntilSettled(t *testing.T, clock *FakeClock, virtualBudget, step, settleBudget time.Duration, cond func() bool) bool {
	t.Helper()
	elapsed := time.Duration(0)
	for elapsed < virtualBudget {
		clock.AdvanceAndSync(step)
		elapsed += step
		if waitForCondition(t, settleBudget, cond) {
			return true
		}
	}
	return false
}

// runElectionUntilConverged advances the shared virtual clock in small
// steps, waiting after each step for the cluster to settle, until
// exactly one leader has emerged or the virtual-time budget is
// exhausted.
func runElectionUntilConverged(t *testing.T, tc *testCluster, virtualBudget, step time.Duration) bool {
	t.Helper()
	return advanceUntilSettled(t, tc.clock, virtualBudget, step, 200*time.Millisecond, func() bool {
		return len(tc.leaders()) == 1
	})
}

// assertConverged checks the standard post-election invariants: exactly
// one leader, every other node a follower, and every node agreeing on
// the same term.
func assertConverged(t *testing.T, tc *testCluster) {
	t.Helper()
	leaders := tc.leaders()
	if len(leaders) != 1 {
		t.Fatalf("got %d leaders (%v), want exactly 1", len(leaders), leaders)
	}
	leaderTerm := tc.cores[leaders[0]].State().CurrentTerm

	for _, id := range tc.ids {
		snap := tc.cores[id].State()
		if id == leaders[0] {
			continue
		}
		if snap.Role != Follower {
			t.Fatalf("node %s: role = %v, want Follower", id, snap.Role)
		}
		if snap.CurrentTerm != leaderTerm {
			t.Fatalf("node %s: term = %d, want %d (leader's term)", id, snap.CurrentTerm, leaderTerm)
		}
	}
}

// TestElectionConvergence_ThreeNodes_SeededRandom exercises real
// jitter (ElectionTimeoutMin != Max) with a fixed random seed per node,
// so the exact sequence of who times out first, who votes for whom, and
// whether any split votes occur is identical on every run — this is
// what makes "passing deterministically on repeated runs" possible
// despite genuinely randomized timeouts.
func TestElectionConvergence_ThreeNodes_SeededRandom(t *testing.T) {
	opts := defaultClusterOpts() // 150-300ms range, seed 1
	tc := newTestCluster(t, 3, opts)

	if !runElectionUntilConverged(t, tc, 2*opts.electionTimeoutMax, 5*time.Millisecond) {
		t.Fatal("cluster did not converge to a single leader within the virtual-time budget")
	}
	assertConverged(t, tc)
}

// TestElectionConvergence_FiveNodes_SeededRandom is the same property
// at a larger, odd cluster size (a majority of 5 is 3, not 2 — this
// exercises the actual vote-counting arithmetic, not just the smallest
// possible quorum).
func TestElectionConvergence_FiveNodes_SeededRandom(t *testing.T) {
	opts := defaultClusterOpts()
	opts.seed = 42
	tc := newTestCluster(t, 5, opts)

	if !runElectionUntilConverged(t, tc, 2*opts.electionTimeoutMax, 5*time.Millisecond) {
		t.Fatal("cluster did not converge to a single leader within the virtual-time budget")
	}
	assertConverged(t, tc)
}

// TestElectionConvergence_IsReproducibleAcrossSeeds runs several
// different fixed seeds and requires every single one to converge —
// this is the property that would catch a subtle correctness bug that
// only manifests for certain random timeout orderings (e.g. a
// three-way-tied first timeout), which a single hard-coded seed could
// get lucky and miss.
func TestElectionConvergence_IsReproducibleAcrossSeeds(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 7, 99, 12345} {
		seed := seed
		t.Run(seedName(seed), func(t *testing.T) {
			opts := defaultClusterOpts()
			opts.seed = seed
			tc := newTestCluster(t, 3, opts)
			if !runElectionUntilConverged(t, tc, 2*opts.electionTimeoutMax, 5*time.Millisecond) {
				t.Fatalf("seed %d: cluster did not converge", seed)
			}
			assertConverged(t, tc)
		})
	}
}

func seedName(seed int64) string {
	switch seed {
	case 1:
		return "seed1"
	case 2:
		return "seed2"
	case 3:
		return "seed3"
	case 7:
		return "seed7"
	case 99:
		return "seed99"
	case 12345:
		return "seed12345"
	default:
		return "seed"
	}
}

// TestElectionConvergence_ReElectsAfterLeaderStops proves the other
// half of Phase 2's scope: not just that a cluster converges once, but
// that it recovers a new leader, in a higher term, after the current
// leader disappears.
func TestElectionConvergence_ReElectsAfterLeaderStops(t *testing.T) {
	opts := defaultClusterOpts()
	tc := newTestCluster(t, 3, opts)

	if !runElectionUntilConverged(t, tc, 2*opts.electionTimeoutMax, 5*time.Millisecond) {
		t.Fatal("initial election did not converge")
	}
	assertConverged(t, tc)

	firstLeader := tc.leaders()[0]
	firstTerm := tc.cores[firstLeader].State().CurrentTerm
	tc.cores[firstLeader].Stop()
	tc.net.Unregister(firstLeader)

	converged := waitForCondition(t, 3*time.Second, func() bool {
		if !runElectionUntilConverged(t, tc, opts.electionTimeoutMax, 5*time.Millisecond) {
			return false
		}
		leaders := tc.leaders()
		return len(leaders) == 1 && leaders[0] != firstLeader
	})
	if !converged {
		t.Fatal("cluster did not re-elect a new leader after the old leader stopped")
	}

	newLeader := tc.leaders()[0]
	newTerm := tc.cores[newLeader].State().CurrentTerm
	if newTerm <= firstTerm {
		t.Fatalf("new leader's term = %d, want > %d (old leader's term)", newTerm, firstTerm)
	}
}

// TestElectionSplitVote_RecoversViaSubsequentElection forces a genuine
// split vote — two candidates, same term, neither able to reach a
// majority — and then confirms the cluster recovers in a later term.
// This is a real, expected Raft scenario (paper §5.2 discusses it
// directly), not an edge case, so it gets its own dedicated test rather
// than being left to chance inside the seeded-random convergence tests
// above, which could pass many times over without ever happening to
// exercise a split.
//
// The split itself is constructed via FakeNetwork.Drop, not by racing
// two candidates' RequestVote goroutines against each other and hoping
// for a particular interleaving. If nodeA and nodeB could both reach
// the same follower, which of them that follower votes for would be a
// genuine, unconstrained goroutine-scheduling race — precisely the kind
// of nondeterminism the rest of this package's design (FakeClock,
// seeded Rand) was built to keep out of these tests. Instead, the
// network topology guarantees each follower is reachable by only ONE
// candidate, so there is exactly one possible outcome, not a race with
// a likely one.
func TestElectionSplitVote_RecoversViaSubsequentElection(t *testing.T) {
	net := NewFakeNetwork()
	clock := NewFakeClock(time.Unix(0, 0))
	ids := []string{"nodeA", "nodeB", "nodeC", "nodeD"}
	cores := make(map[string]*Core)

	newNode := func(id string, minTimeout, maxTimeout time.Duration) *Core {
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
			ElectionTimeoutMin: minTimeout,
			ElectionTimeoutMax: maxTimeout,
			HeartbeatInterval:  10 * time.Millisecond,
		})
		net.Register(id, c)
		cores[id] = c
		go c.Run()
		t.Cleanup(c.Stop)
		return c
	}

	const candidateTimeout = 50 * time.Millisecond
	const voterTimeout = 5 * time.Second // effectively "never" within this test's virtual budget

	// nodeA and nodeB are the two candidates: identical fixed timeouts,
	// so their first timeout fires at the same virtual instant, and both
	// become candidates in the same term — the actual precondition for a
	// split vote. nodeC and nodeD are pure voters for round 1; their
	// timeout is set far beyond this test's budget so they never
	// spontaneously start their own election and complicate the count.
	newNode("nodeA", candidateTimeout, candidateTimeout)
	newNode("nodeB", candidateTimeout, candidateTimeout)
	newNode("nodeC", voterTimeout, voterTimeout)
	newNode("nodeD", voterTimeout, voterTimeout)

	// Round 1 topology: nodeA <-> nodeB is cut (so neither can ever vote
	// for the other, regardless of which of them "gets there first" —
	// eliminating that race entirely, not just making it unlikely).
	// nodeA <-> nodeD and nodeB <-> nodeC are also cut, leaving nodeA
	// reachable only by nodeC, and nodeB reachable only by nodeD. Each
	// candidate ends up with exactly 2 of the 4 votes (self + its one
	// reachable follower) — short of the 3-vote majority a 4-node
	// cluster requires.
	net.Drop("nodeA", "nodeB")
	net.Drop("nodeB", "nodeA")
	net.Drop("nodeA", "nodeD")
	net.Drop("nodeD", "nodeA")
	net.Drop("nodeB", "nodeC")
	net.Drop("nodeC", "nodeB")

	clock.AdvanceAndSync(candidateTimeout)

	splitConfirmed := waitForCondition(t, time.Second, func() bool {
		a, b := cores["nodeA"].State(), cores["nodeB"].State()
		return a.Role == Candidate && b.Role == Candidate &&
			a.CurrentTerm == 1 && b.CurrentTerm == 1
	})
	if !splitConfirmed {
		t.Fatalf("round 1 did not settle into the expected split: nodeA=%+v nodeB=%+v",
			cores["nodeA"].State(), cores["nodeB"].State())
	}
	for _, id := range ids {
		if cores[id].State().Role == Leader {
			t.Fatalf("node %s became leader off a 2-of-4 vote split — safety violation", id)
		}
	}

	// Round 2: heal the network, but isolate nodeB entirely (rather than
	// just reconnecting everyone and hoping nodeA wins the next race) so
	// the recovery outcome is deterministic too. nodeA now reaches every
	// other node and should win outright on its very next scheduled
	// retry — the same fixed 50ms timeout it was already counting down
	// to before the split.
	net.HealAll()
	net.Drop("nodeB", "nodeA")
	net.Drop("nodeA", "nodeB")
	net.Drop("nodeB", "nodeC")
	net.Drop("nodeC", "nodeB")
	net.Drop("nodeB", "nodeD")
	net.Drop("nodeD", "nodeB")

	clock.AdvanceAndSync(candidateTimeout) // nodeA and nodeB's next simultaneous retry, at virtual t=100ms

	recovered := waitForCondition(t, time.Second, func() bool {
		return cores["nodeA"].State().Role == Leader
	})
	if !recovered {
		t.Fatalf("cluster did not recover after the split: nodeA=%+v nodeB=%+v nodeC=%+v nodeD=%+v",
			cores["nodeA"].State(), cores["nodeB"].State(), cores["nodeC"].State(), cores["nodeD"].State())
	}

	final := cores["nodeA"].State()
	if final.CurrentTerm != 2 {
		t.Fatalf("nodeA's term = %d, want exactly 2 (round 1 was term 1; recovery is nodeA's very next retry, term 2)", final.CurrentTerm)
	}
	for _, id := range []string{"nodeB", "nodeC", "nodeD"} {
		if cores[id].State().Role == Leader {
			t.Fatalf("node %s also reports Leader — two leaders in the same cluster is a safety violation", id)
		}
	}
}
