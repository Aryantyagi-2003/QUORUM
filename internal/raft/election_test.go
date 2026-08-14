package raft

import (
	"testing"
	"time"
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

// runElectionUntilConverged advances the shared virtual clock in small
// steps, waiting after each step for the cluster to settle, until
// exactly one leader has emerged or the virtual-time budget is
// exhausted.
func runElectionUntilConverged(t *testing.T, tc *testCluster, virtualBudget, step time.Duration) bool {
	t.Helper()
	elapsed := time.Duration(0)
	for elapsed < virtualBudget {
		tc.clock.AdvanceAndSync(step)
		elapsed += step
		if waitForCondition(t, 200*time.Millisecond, func() bool {
			return len(tc.leaders()) == 1
		}) {
			return true
		}
	}
	return false
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
