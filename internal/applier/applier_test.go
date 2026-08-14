package applier

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/kvstore"
	"github.com/Aryantyagi-2003/Quorum/internal/raft"
	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// waitFor polls cond on a short real interval up to budget -- same
// rationale as internal/raft's own waitForCondition: it waits out
// goroutine scheduling (Applier's own goroutine reacting to a commit
// notification), not virtual time, which is a real single-node Core
// here with no FakeClock involved at all (a single-node cluster commits
// immediately on Propose, so no election timing is exercised by these
// tests).
func waitFor(t *testing.T, budget time.Duration, cond func() bool) bool {
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

// newAppliedSingleNode builds a single-node Core (wins its own election
// immediately, so every Propose commits without needing any peers or
// clock advancement at all) wired to a fresh Applier.
func newAppliedSingleNode(t *testing.T) (*raft.Core, *kvstore.Store, *Applier) {
	t.Helper()
	l, err := storage.OpenLog(t.TempDir())
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	core := raft.NewCore(raft.Config{
		ID:                 "solo",
		Transport:          &raft.FakeTransport{Self: "solo", Network: raft.NewFakeNetwork()},
		Clock:              raft.NewRealClock(),
		Rand:               rand.New(rand.NewSource(1)),
		HardState:          storage.NewHardStateStore(t.TempDir()),
		Log:                l,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 20 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
	})
	go core.Run()
	t.Cleanup(core.Stop)

	if !waitFor(t, 2*time.Second, func() bool { return core.State().Role == raft.Leader }) {
		t.Fatal("solo node never became leader")
	}

	store := kvstore.New()
	a := New(core, store)
	go a.Run()
	t.Cleanup(a.Stop)
	return core, store, a
}

func TestApplier_AppliesCommittedSetInOrder(t *testing.T) {
	core, store, a := newAppliedSingleNode(t)

	cmd, err := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpSet, Key: "k", Value: "v1"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	index, _, isLeader := core.Propose(cmd)
	if !isLeader {
		t.Fatal("solo node reported not leader")
	}

	applied := waitFor(t, 2*time.Second, func() bool { return a.LastApplied() >= index })
	if !applied {
		t.Fatalf("entry %d never applied, LastApplied=%d", index, a.LastApplied())
	}
	if v, found := store.Get("k"); !found || v != "v1" {
		t.Fatalf("store.Get(k) = %q, %v, want v1, true", v, found)
	}
}

func TestApplier_AppliesMultipleEntriesInOrder(t *testing.T) {
	core, store, a := newAppliedSingleNode(t)

	var lastIndex uint64
	for i, val := range []string{"v1", "v2", "v3"} {
		cmd, err := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpSet, Key: "k", Value: val})
		if err != nil {
			t.Fatalf("EncodeCommand: %v", err)
		}
		idx, _, isLeader := core.Propose(cmd)
		if !isLeader {
			t.Fatalf("proposal %d: not leader", i)
		}
		lastIndex = idx
	}

	applied := waitFor(t, 2*time.Second, func() bool { return a.LastApplied() >= lastIndex })
	if !applied {
		t.Fatalf("entries never fully applied, LastApplied=%d, want >= %d", a.LastApplied(), lastIndex)
	}
	// Order matters: if entries were applied out of order, the final
	// value would not deterministically be the last one proposed.
	if v, _ := store.Get("k"); v != "v3" {
		t.Fatalf("store.Get(k) = %q, want v3 (entries must apply in log order)", v)
	}
}

func TestApplier_DeleteAfterSet(t *testing.T) {
	core, store, a := newAppliedSingleNode(t)

	setCmd, _ := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpSet, Key: "k", Value: "v1"})
	core.Propose(setCmd)

	delCmd, _ := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpDelete, Key: "k"})
	delIndex, _, isLeader := core.Propose(delCmd)
	if !isLeader {
		t.Fatal("not leader")
	}

	if !waitFor(t, 2*time.Second, func() bool { return a.LastApplied() >= delIndex }) {
		t.Fatal("delete never applied")
	}
	if _, found := store.Get("k"); found {
		t.Fatal("key should be deleted after apply")
	}
}

func TestApplier_CatchesUpOnAlreadyCommittedEntriesAtStartup(t *testing.T) {
	// Propose entries BEFORE the Applier's goroutine has a chance to
	// see any commit notification, to prove the startup catch-up call
	// in Run (not just the notify-driven loop) is what applies them.
	l, err := storage.OpenLog(t.TempDir())
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	core := raft.NewCore(raft.Config{
		ID:                 "solo",
		Transport:          &raft.FakeTransport{Self: "solo", Network: raft.NewFakeNetwork()},
		Clock:              raft.NewRealClock(),
		Rand:               rand.New(rand.NewSource(1)),
		HardState:          storage.NewHardStateStore(t.TempDir()),
		Log:                l,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 20 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
	})
	go core.Run()
	t.Cleanup(core.Stop)
	if !waitFor(t, 2*time.Second, func() bool { return core.State().Role == raft.Leader }) {
		t.Fatal("solo node never became leader")
	}

	cmd, _ := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpSet, Key: "k", Value: "v1"})
	index, _, isLeader := core.Propose(cmd)
	if !isLeader {
		t.Fatal("not leader")
	}
	if !waitFor(t, 2*time.Second, func() bool { return core.State().CommitIndex >= index }) {
		t.Fatal("entry never committed")
	}

	// Only now does the Applier get created and started -- its startup
	// catch-up must apply the entry without needing a fresh commit
	// notification (none will fire; commitIndex already reflects it).
	store := kvstore.New()
	a := New(core, store)
	go a.Run()
	t.Cleanup(a.Stop)

	if !waitFor(t, 2*time.Second, func() bool { return a.LastApplied() >= index }) {
		t.Fatal("startup catch-up never applied the already-committed entry")
	}
	if v, found := store.Get("k"); !found || v != "v1" {
		t.Fatalf("store.Get(k) = %q, %v, want v1, true", v, found)
	}
}
