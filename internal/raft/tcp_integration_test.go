package raft

import (
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// freePort finds an available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// TestTCPTransport_RealWireRoundTrip is a black-box integration test
// over the REAL TCPTransport/RPCListener wire path, not FakeTransport.
// This is deliberately here to close a real coverage gap: FakeTransport
// calls RPCHandler methods directly, so it never exercises JSON
// encoding, the envelope's "rpc" dispatch tag, or an actual socket at
// all. That gap let a real bug ship silently through every
// FakeTransport-based test in Phases 2 and 3 -- Core constructed
// outgoing RequestVoteArgs/AppendEntriesArgs without ever setting their
// RPC field, so RPCListener's dispatch (which switches on exactly that
// field) read every real RPC as an unrecognized "" name and silently
// dropped the connection, even though the exact same Core logic passed
// hundreds of in-process tests. It was found via live multi-node
// manual verification, not by an automated test; this test exists so
// that specific class of bug can never regress silently again.
//
// Uses RealClock and real timing (not FakeClock/AdvanceAndSync), since
// the point is to exercise actual dial/accept/round-trip latency. That
// makes it the one test in this package with a small, inherent
// dependency on the host not being under extreme contention -- the
// timeouts below are generous specifically to absorb normal CI
// scheduling noise; a host so overloaded that a single localhost RPC
// round trip regularly exceeds multiple seconds (observed directly
// during this project's own manual verification) can still make it
// flake, which is a real environmental limit, not a correctness gap in
// what's being tested.
func TestTCPTransport_RealWireRoundTrip(t *testing.T) {
	ids := []string{"nodeA", "nodeB", "nodeC"}
	raftAddrs := make(map[string]string, len(ids))
	for _, id := range ids {
		raftAddrs[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}

	cores := make(map[string]*Core, len(ids))
	for i, id := range ids {
		l, err := storage.OpenLog(t.TempDir())
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		t.Cleanup(func() { l.Close() })

		peerAddrs := make(map[string]string, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peerAddrs[p] = raftAddrs[p]
			}
		}

		c := NewCore(Config{
			ID:                 id,
			Peers:              excludeID(ids, id),
			Transport:          NewTCPTransport(peerAddrs, 2*time.Second),
			Clock:              NewRealClock(),
			Rand:               rand.New(rand.NewSource(int64(i) + 1)),
			HardState:          storage.NewHardStateStore(t.TempDir()),
			Log:                l,
			ElectionTimeoutMin: 200 * time.Millisecond,
			ElectionTimeoutMax: 400 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		})
		cores[id] = c
		go c.Run()
		t.Cleanup(c.Stop)

		listener := NewRPCListener(c)
		addr := raftAddrs[id]
		go func() { _ = listener.Listen(addr) }() // returns an error only at test teardown; nothing to assert on
	}

	// Give every listener a moment to actually bind before elections
	// start firing RPCs at them.
	time.Sleep(150 * time.Millisecond)

	var leaderID string
	converged := false
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []string
		for id, c := range cores {
			if c.State().Role == Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			leaderID = leaders[0]
			converged = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !converged {
		t.Fatal("cluster never converged to a single leader over the real TCP wire path")
	}

	index, _, isLeader := cores[leaderID].Propose([]byte("hello"))
	if !isLeader {
		t.Fatal("Propose: leader reported isLeader=false")
	}

	committed := false
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cores[leaderID].State().CommitIndex >= index {
			committed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !committed {
		t.Fatal("proposed entry never committed over the real TCP wire path")
	}
}
