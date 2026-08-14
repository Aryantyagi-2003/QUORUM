// Command quorumd runs a Quorum node: the Raft consensus core, the KV
// state machine applier, and the client-facing server, all wired
// together. With no -peer flags it runs as a single-node cluster
// (useful for quick local testing); with -peer flags for every other
// node in the cluster, it participates in real leader election and log
// replication (Phases 2-3).
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/applier"
	"github.com/Aryantyagi-2003/Quorum/internal/kvstore"
	"github.com/Aryantyagi-2003/Quorum/internal/raft"
	"github.com/Aryantyagi-2003/Quorum/internal/server"
	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// peerSpec is one other node, given as "id=raftAddr=clientAddr".
type peerSpec struct {
	id, raftAddr, clientAddr string
}

type peerList []peerSpec

func (p *peerList) String() string { return "" }

func (p *peerList) Set(s string) error {
	parts := strings.Split(s, "=")
	if len(parts) != 3 {
		return fmt.Errorf("expected id=raftAddr=clientAddr, got %q", s)
	}
	*p = append(*p, peerSpec{id: parts[0], raftAddr: parts[1], clientAddr: parts[2]})
	return nil
}

func main() {
	id := flag.String("id", "", "this node's unique ID (required)")
	dataDir := flag.String("data-dir", "./data", "directory for durable state (hardstate.json, log.dat)")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "this node's Raft RPC listen address")
	clientAddr := flag.String("client-addr", "127.0.0.1:8000", "this node's client-facing listen address")
	electionMin := flag.Duration("election-timeout-min", 150*time.Millisecond, "minimum election timeout (paper default 150ms)")
	electionMax := flag.Duration("election-timeout-max", 300*time.Millisecond, "maximum election timeout (paper default 300ms)")
	heartbeat := flag.Duration("heartbeat-interval", 50*time.Millisecond, "leader heartbeat interval (must be well below election-timeout-min)")
	rpcTimeout := flag.Duration("rpc-timeout", 2*time.Second, "timeout for outbound Raft RPCs to peers")
	var peers peerList
	flag.Var(&peers, "peer", "another cluster node, as id=raftAddr=clientAddr (repeat for each peer)")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "quorumd: -id is required")
		os.Exit(2)
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("quorumd: create data dir: %v", err)
	}

	peerIDs := make([]string, 0, len(peers))
	raftAddrs := make(map[string]string, len(peers))
	clientAddrs := make(map[string]string, len(peers)+1)
	clientAddrs[*id] = *clientAddr
	for _, p := range peers {
		peerIDs = append(peerIDs, p.id)
		raftAddrs[p.id] = p.raftAddr
		clientAddrs[p.id] = p.clientAddr
	}

	l, err := storage.OpenLog(*dataDir)
	if err != nil {
		log.Fatalf("quorumd: open log: %v", err)
	}
	defer l.Close()

	core := raft.NewCore(raft.Config{
		ID:        *id,
		Peers:     peerIDs,
		Transport: raft.NewTCPTransport(raftAddrs, *rpcTimeout),
		Clock:     raft.NewRealClock(),
		// Mix the PID into the seed alongside wall-clock time: on a
		// heavily loaded host, several node processes can start close
		// enough together that UnixNano() alone risks correlated (or,
		// under coarse clock resolution, identical) seeds across
		// independent processes -- which would defeat the whole point
		// of randomized election timeouts breaking ties between them.
		Rand:               rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(os.Getpid()))),
		HardState:          storage.NewHardStateStore(*dataDir),
		Log:                l,
		ElectionTimeoutMin: *electionMin,
		ElectionTimeoutMax: *electionMax,
		HeartbeatInterval:  *heartbeat,
	})
	go core.Run()

	rpcListener := raft.NewRPCListener(core)
	go func() {
		if err := rpcListener.Listen(*raftAddr); err != nil {
			log.Fatalf("quorumd: raft listener: %v", err)
		}
	}()

	store := kvstore.New()
	ap := applier.New(core, store)
	go ap.Run()

	log.Printf("quorumd: node %q up — raft on %s, client on %s, %d peer(s)", *id, *raftAddr, *clientAddr, len(peerIDs))
	srv := server.New(*clientAddr, core, ap, store, clientAddrs)
	if err := srv.Listen(); err != nil {
		log.Fatalf("quorumd: %v", err)
	}
}
