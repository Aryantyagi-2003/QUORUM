package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// testCluster is a set of Cores wired together over one FakeNetwork and
// one shared FakeClock, entirely in-process — no real sockets, no real
// timers.
type testCluster struct {
	net   *FakeNetwork
	clock *FakeClock
	cores map[string]*Core
	ids   []string
}

type clusterOpts struct {
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	seed               int64
}

func defaultClusterOpts() clusterOpts {
	return clusterOpts{
		electionTimeoutMin: 150 * time.Millisecond,
		electionTimeoutMax: 300 * time.Millisecond,
		heartbeatInterval:  20 * time.Millisecond,
		seed:               1,
	}
}

// newTestCluster wires n nodes ("node0".."nodeN-1") together and starts
// each Core's Run loop. It does not advance the clock; callers drive
// elections via clock.AdvanceAndSync.
func newTestCluster(t *testing.T, n int, opts clusterOpts) *testCluster {
	t.Helper()
	net := NewFakeNetwork()
	clock := NewFakeClock(time.Unix(0, 0))

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}

	tc := &testCluster{net: net, clock: clock, cores: make(map[string]*Core), ids: ids}
	for i, id := range ids {
		peers := excludeID(ids, id)
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
			Rand:               rand.New(rand.NewSource(opts.seed + int64(i))),
			HardState:          storage.NewHardStateStore(t.TempDir()),
			Log:                l,
			ElectionTimeoutMin: opts.electionTimeoutMin,
			ElectionTimeoutMax: opts.electionTimeoutMax,
			HeartbeatInterval:  opts.heartbeatInterval,
		})
		net.Register(id, c)
		tc.cores[id] = c
		go c.Run()
	}
	t.Cleanup(func() {
		for _, c := range tc.cores {
			c.Stop()
		}
	})
	return tc
}

func excludeID(ids []string, self string) []string {
	out := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// leaders returns the IDs of every node currently reporting Role ==
// Leader.
func (tc *testCluster) leaders() []string {
	var out []string
	for _, id := range tc.ids {
		if tc.cores[id].State().Role == Leader {
			out = append(out, id)
		}
	}
	return out
}
