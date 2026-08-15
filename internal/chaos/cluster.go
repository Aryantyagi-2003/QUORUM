package chaos

import (
	"fmt"
	"net"
	"os"
	"time"
)

// ClusterConfig describes the cluster to stand up. Every node uses the
// same timeouts (deliberately the project's real, unmodified defaults
// -- 150-300ms election, 50ms heartbeat -- unless a scenario has a
// specific reason to override them; inflating timeouts to paper over
// host contention is exactly what this project's own debugging
// discipline (Phase 4) ruled out).
type ClusterConfig struct {
	NodeIDs                  []string
	BaseDir                  string // each node gets BaseDir/<id>
	BinaryPath               string // path to a built quorumd binary
	ElectionMin, ElectionMax time.Duration
	Heartbeat, RPCTimeout    time.Duration
}

// Cluster is a standing set of real quorumd processes wired together
// through a ProxyNetwork -- every inter-node Raft connection passes
// through a real per-directed-pair proxy, so scenarios that never call
// Drop/Partition still exercise the exact same topology as ones that
// do (and incidentally prove the proxy layer itself is transparent to
// normal operation).
type Cluster struct {
	cfg   ClusterConfig
	Nodes map[string]*Node
	Net   *ProxyNetwork

	// ClientAddrs maps node ID -> real client-facing address (never
	// proxied -- chaos scenarios partition nodes from each other, not
	// clients from the cluster).
	ClientAddrs map[string]string
}

// StandUp allocates ports, wires up the proxy mesh, writes each node's
// config, and starts every process. Returns once every process has
// been launched (not once the cluster has elected a leader -- callers
// wait for that explicitly, since how long it takes is often itself
// what's being measured).
func StandUp(cfg ClusterConfig) (*Cluster, error) {
	if err := os.MkdirAll(cfg.BaseDir, 0o755); err != nil {
		return nil, fmt.Errorf("chaos: mkdir base dir: %w", err)
	}

	realRaftAddrs := make(map[string]string, len(cfg.NodeIDs))
	clientAddrs := make(map[string]string, len(cfg.NodeIDs))
	for _, id := range cfg.NodeIDs {
		realRaftAddrs[id] = fmt.Sprintf("127.0.0.1:%d", mustFreePort())
		clientAddrs[id] = fmt.Sprintf("127.0.0.1:%d", mustFreePort())
	}

	net_ := NewProxyNetwork()
	// proxyAddrFor[from][to] = the address "from" should dial to reach
	// "to" -- a dedicated proxy per ordered pair, forwarding to to's
	// real Raft address.
	proxyAddrFor := make(map[string]map[string]string, len(cfg.NodeIDs))
	for _, from := range cfg.NodeIDs {
		proxyAddrFor[from] = make(map[string]string, len(cfg.NodeIDs)-1)
		for _, to := range cfg.NodeIDs {
			if from == to {
				continue
			}
			listenAddr := fmt.Sprintf("127.0.0.1:%d", mustFreePort())
			p, err := NewProxy(from, to, listenAddr, realRaftAddrs[to])
			if err != nil {
				net_.Close()
				return nil, fmt.Errorf("chaos: proxy %s->%s: %w", from, to, err)
			}
			net_.Add(from, to, p)
			proxyAddrFor[from][to] = listenAddr
		}
	}

	nodes := make(map[string]*Node, len(cfg.NodeIDs))
	for _, id := range cfg.NodeIDs {
		var peers []PeerSpec
		for _, peerID := range cfg.NodeIDs {
			if peerID == id {
				continue
			}
			peers = append(peers, PeerSpec{
				ID:         peerID,
				RaftAddr:   proxyAddrFor[id][peerID], // dial the proxy, not the peer directly
				ClientAddr: clientAddrs[peerID],
			})
		}
		node := NewNode(NodeConfig{
			ID:          id,
			DataDir:     cfg.BaseDir + "/" + id,
			RaftAddr:    realRaftAddrs[id],
			ClientAddr:  clientAddrs[id],
			Peers:       peers,
			ElectionMin: cfg.ElectionMin,
			ElectionMax: cfg.ElectionMax,
			Heartbeat:   cfg.Heartbeat,
			RPCTimeout:  cfg.RPCTimeout,
			BinaryPath:  cfg.BinaryPath,
		})
		nodes[id] = node
	}

	c := &Cluster{cfg: cfg, Nodes: nodes, Net: net_, ClientAddrs: clientAddrs}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

// AllClientAddrs returns every node's client-facing address, in
// NodeIDs order -- the slice a client.Client should be constructed
// with.
func (c *Cluster) AllClientAddrs() []string {
	addrs := make([]string, 0, len(c.cfg.NodeIDs))
	for _, id := range c.cfg.NodeIDs {
		addrs = append(addrs, c.ClientAddrs[id])
	}
	return addrs
}

// Close kills every node and closes every proxy. Safe to call even if
// StandUp partially failed.
func (c *Cluster) Close() {
	for _, node := range c.Nodes {
		node.Kill()
	}
	if c.Net != nil {
		c.Net.Close()
	}
}

// mustFreePort asks the OS for an ephemeral port by briefly binding to
// port 0 and reading back what it was assigned, then releasing it.
// There's an inherent, unavoidable TOCTOU gap between this and the
// real listener binding the same port (something else could grab it
// in between) -- acceptable for a local test harness, not something a
// production system would do.
func mustFreePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("chaos: allocate port: %v", err))
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
