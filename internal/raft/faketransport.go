package raft

import (
	"errors"
	"sync"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

// ErrUnreachable is returned by FakeTransport when the target node is
// unregistered (simulating a crashed process) or the link between the
// two nodes is currently dropped (simulating a network partition).
var ErrUnreachable = errors.New("raft: peer unreachable")

// FakeNetwork is a shared, in-process registry of node IDs to
// RPCHandlers, with fault injection. All nodes in a test share one
// FakeNetwork. Calling through it is a direct function call on the
// caller's own goroutine — no socket, no serialization — which keeps
// election-logic tests fast and focused on Raft's decision rules
// rather than the wire format (already covered independently by
// internal/proto's own tests).
//
// Drop/Partition/Heal are the same fault-injection primitives Phase 5's
// chaos harness will need later; building them here means that harness
// can reuse this exact mechanism for its in-process scenarios rather
// than inventing a second one.
type FakeNetwork struct {
	mu       sync.Mutex
	handlers map[string]RPCHandler
	dropped  map[[2]string]bool // (from, to) pairs currently dropped, one direction each
}

func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{
		handlers: make(map[string]RPCHandler),
		dropped:  make(map[[2]string]bool),
	}
}

// Register makes nodeID reachable via h. Call once per node at cluster
// setup.
func (n *FakeNetwork) Register(nodeID string, h RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[nodeID] = h
}

// Unregister makes nodeID permanently unreachable, simulating a crashed
// process (as opposed to Drop, which simulates a network problem
// between two otherwise-healthy nodes).
func (n *FakeNetwork) Unregister(nodeID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, nodeID)
}

// Drop blocks RPCs sent from -> to, in that direction only. Call twice
// (both directions) to simulate a link being down entirely.
func (n *FakeNetwork) Drop(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropped[[2]string{from, to}] = true
}

// Heal reverses a prior Drop for from -> to.
func (n *FakeNetwork) Heal(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.dropped, [2]string{from, to})
}

// Partition drops every cross-group RPC, both directions, between
// groupA and groupB — the two resulting halves can no longer reach each
// other at all, though each remains fully connected internally.
func (n *FakeNetwork) Partition(groupA, groupB []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			n.dropped[[2]string{a, b}] = true
			n.dropped[[2]string{b, a}] = true
		}
	}
}

// HealAll clears every dropped link, restoring full connectivity.
func (n *FakeNetwork) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropped = make(map[[2]string]bool)
}

func (n *FakeNetwork) isDropped(from, to string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dropped[[2]string{from, to}]
}

func (n *FakeNetwork) handlerFor(nodeID string) (RPCHandler, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h, ok := n.handlers[nodeID]
	return h, ok
}

// FakeTransport is the per-node handle a Core uses to reach a shared
// FakeNetwork.
type FakeTransport struct {
	Self    string
	Network *FakeNetwork
}

func (t *FakeTransport) RequestVote(peerID string, args *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	h, err := t.resolve(peerID)
	if err != nil {
		return nil, err
	}
	reply := h.HandleRequestVote(args)
	if reply == nil {
		return nil, ErrUnreachable
	}
	return reply, nil
}

func (t *FakeTransport) AppendEntries(peerID string, args *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	h, err := t.resolve(peerID)
	if err != nil {
		return nil, err
	}
	reply := h.HandleAppendEntries(args)
	if reply == nil {
		return nil, ErrUnreachable
	}
	return reply, nil
}

func (t *FakeTransport) resolve(peerID string) (RPCHandler, error) {
	if t.Network.isDropped(t.Self, peerID) {
		return nil, ErrUnreachable
	}
	h, ok := t.Network.handlerFor(peerID)
	if !ok {
		return nil, ErrUnreachable
	}
	return h, nil
}
