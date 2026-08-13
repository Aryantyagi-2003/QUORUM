// Package kvstore implements Quorum's state machine: an in-memory map
// that GET/SET/DELETE commands are applied to. In the full system this
// is the thing Raft's committed log entries get applied to, one at a
// time, in log order — never mutated any other way, so that any two
// nodes which have applied the same log prefix have identical state.
package kvstore

import (
	"encoding/json"
	"fmt"
	"sync"
)

// CommandOp identifies the kind of mutation a Command performs.
type CommandOp string

const (
	OpSet    CommandOp = "set"
	OpDelete CommandOp = "delete"
)

// Command is the opaque payload carried inside a proto.LogEntry.Command
// for KV mutations. GET is not a Command: it never goes through the
// log (see Store.Get), since it doesn't mutate state.
type Command struct {
	Op    CommandOp `json:"op"`
	Key   string    `json:"key"`
	Value string    `json:"value,omitempty"`

	// ClientID/SeqNum echo the request that produced this command, so
	// Store.Apply can deduplicate a command that was appended to the
	// log twice (e.g. a client retry after an ambiguous leader
	// failover) — see Raft paper §8.
	ClientID string `json:"clientId,omitempty"`
	SeqNum   uint64 `json:"seqNum,omitempty"`
}

func EncodeCommand(c Command) ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCommand(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// Store is the in-memory KV state machine. It is safe for concurrent
// use: Apply is called by a single applier goroutine as entries
// commit, while Get may be called concurrently from request-handling
// goroutines.
type Store struct {
	mu   sync.RWMutex
	data map[string]string

	// lastSeq tracks the highest SeqNum applied per ClientID, so a
	// duplicate command (same ClientID+SeqNum) is a no-op rather than
	// being applied twice.
	lastSeq map[string]uint64
}

func New() *Store {
	return &Store{
		data:    make(map[string]string),
		lastSeq: make(map[string]uint64),
	}
}

// Get reads a key from local state. Callers decide whether it's valid
// to serve this read (e.g. only on the leader, for linearizability —
// see the server package for that policy); Store itself has no notion
// of leadership.
func (s *Store) Get(key string) (value string, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Apply applies a committed command to the state machine. It must only
// be called with commands from entries that have actually committed
// (a majority has replicated them), in log order, exactly once per
// index — the applier goroutine owns this invariant.
func (s *Store) Apply(c Command) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c.ClientID != "" {
		if last, ok := s.lastSeq[c.ClientID]; ok && c.SeqNum <= last {
			return // already applied (duplicate from a client retry)
		}
		s.lastSeq[c.ClientID] = c.SeqNum
	}

	switch c.Op {
	case OpSet:
		s.data[c.Key] = c.Value
	case OpDelete:
		delete(s.data, c.Key)
	default:
		panic(fmt.Sprintf("kvstore: unknown command op %q", c.Op))
	}
}
