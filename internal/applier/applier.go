// Package applier applies a raft.Core's committed log entries to a
// kvstore.Store, in order, exactly once each. It is the boundary
// between generic Raft consensus and this project's specific state
// machine: Core knows nothing about KV commands (it only ever sees
// opaque []byte), and Applier is the only thing that decodes them.
package applier

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Aryantyagi-2003/Quorum/internal/kvstore"
	"github.com/Aryantyagi-2003/Quorum/internal/raft"
)

// Applier drives entries from a Core's committed log into a
// kvstore.Store as commitIndex advances.
type Applier struct {
	core  *raft.Core
	store *kvstore.Store

	// lastApplied is read from other goroutines (a client-facing
	// server's write-wait path) without going through any channel, so
	// it's a plain atomic rather than state owned by Applier's own
	// goroutine the way Core's fields are owned by Core's.
	lastApplied atomic.Uint64

	stopCh   chan struct{}
	stopOnce sync.Once
}

func New(core *raft.Core, store *kvstore.Store) *Applier {
	return &Applier{core: core, store: store, stopCh: make(chan struct{})}
}

// Run drives the Applier until Stop is called. Call it in its own
// goroutine.
func (a *Applier) Run() {
	a.applyNewlyCommitted() // catch up on anything already committed at startup
	for {
		select {
		case <-a.core.CommitNotifyChan():
			a.applyNewlyCommitted()
		case <-a.stopCh:
			return
		}
	}
}

func (a *Applier) Stop() {
	a.stopOnce.Do(func() { close(a.stopCh) })
}

// LastApplied returns the highest log index applied so far. Safe to
// call from any goroutine.
func (a *Applier) LastApplied() uint64 {
	return a.lastApplied.Load()
}

func (a *Applier) applyNewlyCommitted() {
	target := a.core.State().CommitIndex
	for a.lastApplied.Load() < target {
		next := a.lastApplied.Load() + 1
		entry, ok := a.core.LogEntry(next)
		if !ok {
			// A committed index is guaranteed by Raft's safety
			// properties to exist in the log of any node that knows
			// it's committed -- a miss here means Core's invariants
			// have already been violated elsewhere, not something
			// Applier can recover from.
			panic(fmt.Sprintf("applier: committed entry %d missing from log", next))
		}
		cmd, err := kvstore.DecodeCommand(entry.Command)
		if err != nil {
			// Every command in the log was encoded by this same
			// project's server via kvstore.EncodeCommand before being
			// handed to Core.Propose -- a decode failure means
			// corruption, not a recoverable runtime condition.
			panic(fmt.Sprintf("applier: entry %d: corrupt command: %v", next, err))
		}
		a.store.Apply(cmd)
		a.lastApplied.Store(next)
	}
}
