package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

// Transport lets a Core send RPCs to a peer without knowing whether
// that peer is reached over a real TCP connection or an in-process
// fake — see FakeTransport for the test-only implementation.
type Transport interface {
	RequestVote(peerID string, args *proto.RequestVoteArgs) (*proto.RequestVoteReply, error)
	AppendEntries(peerID string, args *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error)
}

// RPCHandler is how an inbound RPC reaches a node's Core, regardless of
// transport — the real TCP listener and FakeTransport both just call
// these two methods; neither knows anything about Raft internals beyond
// this. A nil return means the node is no longer accepting RPCs (e.g.
// its Core has stopped); callers should treat that the same as a
// network error.
type RPCHandler interface {
	HandleRequestVote(args *proto.RequestVoteArgs) *proto.RequestVoteReply
	HandleAppendEntries(args *proto.AppendEntriesArgs) *proto.AppendEntriesReply
}

// TCPTransport is the production Transport: it dials a peer's Raft
// address fresh per RPC using proto's length-prefixed JSON framing.
// Dial-per-request rather than a pooled connection, mirroring
// quorumctl's client model — Raft RPCs (heartbeats on the order of tens
// of milliseconds, elections rarer still) are far too infrequent for
// connection pooling to matter, and dial-per-request keeps failure
// handling trivial (a dead peer just fails to dial).
type TCPTransport struct {
	addrs   map[string]string // peer ID -> "host:port"
	timeout time.Duration
}

func NewTCPTransport(addrs map[string]string, timeout time.Duration) *TCPTransport {
	return &TCPTransport{addrs: addrs, timeout: timeout}
}

func (t *TCPTransport) RequestVote(peerID string, args *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	var reply proto.RequestVoteReply
	if err := t.call(peerID, args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *TCPTransport) AppendEntries(peerID string, args *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	var reply proto.AppendEntriesReply
	if err := t.call(peerID, args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *TCPTransport) call(peerID string, args any, reply any) error {
	addr, ok := t.addrs[peerID]
	if !ok {
		return fmt.Errorf("raft: unknown peer %q", peerID)
	}
	conn, err := net.DialTimeout("tcp", addr, t.timeout)
	if err != nil {
		return fmt.Errorf("raft: dial %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(t.timeout))

	if err := proto.WriteMessage(conn, args); err != nil {
		return fmt.Errorf("raft: send to %s: %w", addr, err)
	}
	if err := proto.ReadMessage(conn, reply); err != nil {
		return fmt.Errorf("raft: read from %s: %w", addr, err)
	}
	return nil
}

// RPCListener accepts inbound Raft RPCs on a TCP address and dispatches
// them to an RPCHandler — the server-side counterpart to TCPTransport.
type RPCListener struct {
	handler RPCHandler
}

func NewRPCListener(handler RPCHandler) *RPCListener {
	return &RPCListener{handler: handler}
}

// Listen blocks accepting connections until the listener is closed.
func (l *RPCListener) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("raft: listen: %w", err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go l.handleConn(conn)
	}
}

func (l *RPCListener) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		body, err := proto.ReadFrame(conn)
		if err != nil {
			return
		}
		var env proto.Envelope
		if err := json.Unmarshal(body, &env); err != nil {
			return
		}
		switch env.RPC {
		case "RequestVote":
			var args proto.RequestVoteArgs
			if err := json.Unmarshal(body, &args); err != nil {
				return
			}
			reply := l.handler.HandleRequestVote(&args)
			if reply == nil || proto.WriteMessage(conn, reply) != nil {
				return
			}
		case "AppendEntries":
			var args proto.AppendEntriesArgs
			if err := json.Unmarshal(body, &args); err != nil {
				return
			}
			reply := l.handler.HandleAppendEntries(&args)
			if reply == nil || proto.WriteMessage(conn, reply) != nil {
				return
			}
		default:
			return
		}
	}
}
