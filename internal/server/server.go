// Package server implements Quorum's client-facing TCP listener.
//
// This file (server.go) is the Phase 1 single-node harness: it wires
// client requests straight through the durable log and into the KV
// state machine with no Raft consensus involved (term is hard-coded to
// 1, there is exactly one "node" and it always accepts writes). Its
// purpose is to validate the storage engine and wire protocol in
// isolation before leader election and replication are layered in, per
// the project's staged build plan. Once the Raft core lands, this
// dispatch will be replaced by routing writes through the consensus
// module instead of appending directly.
package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/Aryantyagi-2003/Quorum/internal/kvstore"
	"github.com/Aryantyagi-2003/Quorum/internal/proto"
	"github.com/Aryantyagi-2003/Quorum/internal/storage"
)

// singleNodeTerm is the fixed log term used before Raft leader election
// exists. Real terms start at 0 and increase via elections; Phase 1
// has no elections, so every entry is written as term 1.
const singleNodeTerm = 1

type Server struct {
	addr string
	log  *storage.Log
	kv   *kvstore.Store

	mu        sync.Mutex // serializes writes (append+apply must be atomic together)
	nextIndex uint64
}

// Open opens (or creates) the on-disk log in dataDir, replays it into
// a fresh KV store, and returns a Server ready to Listen. Replaying on
// every startup is what makes state survive a process restart.
func Open(addr, dataDir string) (*Server, error) {
	l, err := storage.OpenLog(dataDir)
	if err != nil {
		return nil, fmt.Errorf("server: open log: %w", err)
	}

	kv := kvstore.New()
	last := l.LastIndex()
	for i := uint64(1); i <= last; i++ {
		entry, ok := l.Get(i)
		if !ok {
			return nil, fmt.Errorf("server: replay: missing log entry at index %d", i)
		}
		cmd, err := kvstore.DecodeCommand(entry.Command)
		if err != nil {
			return nil, fmt.Errorf("server: replay: decode entry %d: %w", i, err)
		}
		kv.Apply(cmd)
	}

	return &Server{
		addr:      addr,
		log:       l,
		kv:        kv,
		nextIndex: last + 1,
	}, nil
}

func (s *Server) Close() error {
	return s.log.Close()
}

// Listen accepts client connections until the listener is closed or
// listening fails. It blocks; run it in a goroutine or as the last
// call in main.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("server: listen: %w", err)
	}
	log.Printf("quorumd: listening on %s", s.addr)
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
		value, found := s.kv.Get(req.Key)
		return proto.ClientResponse{OK: true, Value: value, Found: found}
	case "Set":
		return s.write(kvstore.Command{
			Op: kvstore.OpSet, Key: req.Key, Value: req.Value,
			ClientID: req.ClientID, SeqNum: req.SeqNum,
		})
	case "Delete":
		return s.write(kvstore.Command{
			Op: kvstore.OpDelete, Key: req.Key,
			ClientID: req.ClientID, SeqNum: req.SeqNum,
		})
	default:
		return proto.ClientResponse{OK: false, Error: fmt.Sprintf("unknown rpc %q", req.RPC)}
	}
}

// write appends cmd to the durable log, fsyncing before applying it to
// the state machine or acknowledging the client — the same
// "persist before respond" rule the full Raft core will use for
// AppendEntries.
func (s *Server) write(cmd kvstore.Command) proto.ClientResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	encoded, err := kvstore.EncodeCommand(cmd)
	if err != nil {
		return proto.ClientResponse{OK: false, Error: err.Error()}
	}
	entry := proto.LogEntry{Term: singleNodeTerm, Index: s.nextIndex, Command: encoded}
	if err := s.log.Append([]proto.LogEntry{entry}); err != nil {
		return proto.ClientResponse{OK: false, Error: err.Error()}
	}
	s.nextIndex++
	s.kv.Apply(cmd)
	return proto.ClientResponse{OK: true}
}
