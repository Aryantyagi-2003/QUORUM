// Package chaos implements Quorum's Phase 5 chaos-testing harness:
// orchestrating real quorumd processes (real SIGKILL, real restart
// from the same data directory) and real TCP proxies for network-
// partition fault injection, plus a client-write-log verifier that
// checks cluster correctness programmatically rather than by manual
// inspection.
//
// Everything here drives real processes over real sockets, not
// internal/raft's FakeTransport/FakeNetwork -- Phase 4 proved that
// matters: a real wire-protocol bug (a missing envelope tag) was
// invisible to hundreds of FakeTransport-based tests and only surfaced
// under live verification. The chaos harness exists specifically to
// keep exercising that real path.
package chaos

import (
	"io"
	"log"
	"net"
	"sync/atomic"
)

// Proxy forwards TCP connections from a listen address to a target
// address, and can be commanded to refuse new connections -- the
// harness's mechanism for simulating a network partition between two
// real quorumd processes without touching either of them directly.
//
// The drop flag is only checked at Accept time, not mid-stream: a
// connection already past that check when Drop is called is allowed
// to finish its single request/reply (Quorum's TCPTransport dials
// fresh per RPC and closes immediately after, so these are always
// short-lived). This is a deliberate scope decision, not an oversight
// -- see the scenario 2 design notes on the settling-delay window this
// implies for the verifier.
type Proxy struct {
	from, to string // node IDs, for logging/identification
	ln       net.Listener
	target   string
	dropped  atomic.Bool
}

// NewProxy starts listening on listenAddr and forwarding accepted
// connections to targetAddr, labeled from/to for logging.
func NewProxy(from, to, listenAddr, targetAddr string) (*Proxy, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{from: from, to: to, ln: ln, target: targetAddr}
	go p.acceptLoop()
	return p, nil
}

func (p *Proxy) acceptLoop() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return // listener closed
		}
		if p.dropped.Load() {
			conn.Close() // simulate unreachable: refuse immediately, same as TCPTransport already handles a dead/unreachable peer
			continue
		}
		go p.forward(conn)
	}
}

func (p *Proxy) forward(conn net.Conn) {
	defer conn.Close()
	target, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer target.Close()

	done := make(chan struct{}, 2)
	go func() { io.Copy(target, conn); done <- struct{}{} }()
	go func() { io.Copy(conn, target); done <- struct{}{} }()
	<-done
}

// Drop refuses every new connection through this proxy from now on
// (existing in-flight connections still finish -- see the type doc).
func (p *Proxy) Drop() { p.dropped.Store(true) }

// Heal resumes forwarding new connections.
func (p *Proxy) Heal() { p.dropped.Store(false) }

func (p *Proxy) Addr() string { return p.ln.Addr().String() }

func (p *Proxy) Close() error { return p.ln.Close() }

// ProxyNetwork is the full set of ordered-pair proxies for a cluster --
// one Proxy per (from, to) direction, so Drop(A,B) and Drop(B,A) are
// independently controllable, exactly mirroring internal/raft's
// FakeNetwork API (same vocabulary, real sockets underneath instead of
// direct function calls).
type ProxyNetwork struct {
	proxies map[[2]string]*Proxy // (from, to) -> proxy
}

func NewProxyNetwork() *ProxyNetwork {
	return &ProxyNetwork{proxies: make(map[[2]string]*Proxy)}
}

// Add registers the proxy a node "from" uses to reach node "to".
func (n *ProxyNetwork) Add(from, to string, p *Proxy) {
	n.proxies[[2]string{from, to}] = p
}

func (n *ProxyNetwork) Drop(from, to string) {
	if p, ok := n.proxies[[2]string{from, to}]; ok {
		p.Drop()
	} else {
		log.Printf("chaos: Drop(%s,%s): no proxy registered", from, to)
	}
}

func (n *ProxyNetwork) Heal(from, to string) {
	if p, ok := n.proxies[[2]string{from, to}]; ok {
		p.Heal()
	}
}

// Partition drops every cross-group link, both directions, between
// groupA and groupB.
func (n *ProxyNetwork) Partition(groupA, groupB []string) {
	for _, a := range groupA {
		for _, b := range groupB {
			n.Drop(a, b)
			n.Drop(b, a)
		}
	}
}

// HealAll restores every link.
func (n *ProxyNetwork) HealAll() {
	for _, p := range n.proxies {
		p.Heal()
	}
}

func (n *ProxyNetwork) Close() {
	for _, p := range n.proxies {
		p.Close()
	}
}
