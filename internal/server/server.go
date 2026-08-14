// Package server implements Quorum's client-facing TCP listener.
//
// Phase 4: client writes go through raft.Core.Propose and are only
// acknowledged once actually applied (via internal/applier) to the KV
// state machine — replacing Phase 1's direct-to-log write path, which
// had no consensus at all. Reads are served only from the leader
// (Phase 0's documented linearizability-over-follower-read-scalability
// tradeoff): a non-leader returns "not leader" plus a LeaderHint the
// client can redirect to, exactly like a write would.
package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/applier"
	"github.com/Aryantyagi-2003/Quorum/internal/kvstore"
	"github.com/Aryantyagi-2003/Quorum/internal/proto"
	"github.com/Aryantyagi-2003/Quorum/internal/raft"
)

const defaultWriteTimeout = 2 * time.Second

// errLostLeadership means this node stopped being leader (or moved to
// a later term) before a proposed entry was confirmed applied. The
// entry may or may not still commit under new leadership — leader
// completeness (proven in Phase 3) guarantees it isn't silently lost if
// a majority already had it — but this node can no longer promise
// anything about it, so the client is told to redirect and retry.
var errLostLeadership = errors.New("server: lost leadership before entry applied")
var errApplyTimeout = errors.New("server: timed out waiting for entry to apply")

type Server struct {
	addr    string
	core    *raft.Core
	applier *applier.Applier
	store   *kvstore.Store

	// clientAddrs maps a Raft node ID to that node's client-facing
	// address, so a "not leader" response's LeaderHint is something a
	// client can actually dial — Core only ever knows the leader's
	// Raft node ID, not its client-facing address.
	clientAddrs map[string]string

	writeTimeout time.Duration
}

func New(addr string, core *raft.Core, ap *applier.Applier, store *kvstore.Store, clientAddrs map[string]string) *Server {
	return &Server{
		addr:         addr,
		core:         core,
		applier:      ap,
		store:        store,
		clientAddrs:  clientAddrs,
		writeTimeout: defaultWriteTimeout,
	}
}

// Listen accepts client connections until the listener is closed or
// listening fails. It blocks; run it in a goroutine or as the last
// call in main.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("server: listen: %w", err)
	}
	log.Printf("quorumd: client listener on %s", s.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		var req proto.ClientRequest
		if err := proto.ReadMessage(conn, &req); err != nil {
			if err != io.EOF {
				log.Printf("quorumd: read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		resp := s.dispatch(req)
		if err := proto.WriteMessage(conn, resp); err != nil {
			log.Printf("quorumd: write error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (s *Server) dispatch(req proto.ClientRequest) proto.ClientResponse {
	switch req.RPC {
	case "Get":
		snap := s.core.State()
		if snap.Role != raft.Leader {
			return s.notLeaderResponse(snap)
		}
		value, found := s.store.Get(req.Key)
		return proto.ClientResponse{OK: true, Value: value, Found: found}
	case "Set":
		return s.proposeAndWait(kvstore.Command{
			Op: kvstore.OpSet, Key: req.Key, Value: req.Value,
			ClientID: req.ClientID, SeqNum: req.SeqNum,
		})
	case "Delete":
		return s.proposeAndWait(kvstore.Command{
			Op: kvstore.OpDelete, Key: req.Key,
			ClientID: req.ClientID, SeqNum: req.SeqNum,
		})
	default:
		return proto.ClientResponse{OK: false, Error: fmt.Sprintf("unknown rpc %q", req.RPC)}
	}
}

func (s *Server) notLeaderResponse(snap raft.Snapshot) proto.ClientResponse {
	if snap.LeaderID == "" {
		return proto.ClientResponse{OK: false, Error: "no leader"}
	}
	return proto.ClientResponse{OK: false, Error: "not leader", LeaderHint: s.clientAddrs[snap.LeaderID]}
}

// proposeAndWait appends cmd to the leader's log via Core.Propose and
// blocks until the Applier has actually applied it (not merely
// committed it — see waitForApplied) before acknowledging the client,
// so a client that gets OK:true is guaranteed its own write is already
// visible to a subsequent Get on this same node.
func (s *Server) proposeAndWait(cmd kvstore.Command) proto.ClientResponse {
	encoded, err := kvstore.EncodeCommand(cmd)
	if err != nil {
		return proto.ClientResponse{OK: false, Error: err.Error()}
	}
	index, term, isLeader := s.core.Propose(encoded)
	if !isLeader {
		return s.notLeaderResponse(s.core.State())
	}
	if err := s.waitForApplied(index, term); err != nil {
		if errors.Is(err, errLostLeadership) {
			return s.notLeaderResponse(s.core.State())
		}
		return proto.ClientResponse{OK: false, Error: "timeout"}
	}
	return proto.ClientResponse{OK: true}
}

// waitForApplied blocks until index has been applied by the Applier
// (not just committed by Core — see the package doc), or returns an
// error explaining why it gave up. Gating on the Applier's progress
// rather than raw commitIndex is what gives a client read-your-writes:
// if this only waited for commitIndex, a client's Get sent immediately
// after a successful Set could race the Applier and miss its own
// write.
func (s *Server) waitForApplied(index, proposeTerm uint64) error {
	deadline := time.Now().Add(s.writeTimeout)
	for {
		if s.applier.LastApplied() >= index {
			return nil
		}
		snap := s.core.State()
		if snap.Role != raft.Leader || snap.CurrentTerm != proposeTerm {
			return errLostLeadership
		}
		if time.Now().After(deadline) {
			return errApplyTimeout
		}
		select {
		case <-s.core.CommitNotifyChan():
		case <-time.After(5 * time.Millisecond): // safety-net poll in case a signal was coalesced away
		}
	}
}
