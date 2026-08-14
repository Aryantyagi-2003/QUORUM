// Package raft implements Quorum's Raft consensus core: leader election
// (Raft paper §5.2) in this phase, with log replication and commit
// tracking layered on top in later phases.
package raft

import "time"

// Clock abstracts time so Core's election-timeout and heartbeat logic
// can be driven deterministically in tests instead of by the real wall
// clock — the same principle as Transport: Core never touches real
// infrastructure directly, only injected interfaces.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
}

// Timer mirrors the subset of *time.Timer that Core needs. C is a
// method (not a field, unlike time.Timer.C) so both the real and fake
// implementations can satisfy the same interface.
type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration) bool
	Stop() bool
}

// Ticker mirrors the subset of *time.Ticker that Core needs.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock is the production Clock, wrapping the standard library.
type RealClock struct{}

func NewRealClock() RealClock { return RealClock{} }

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

func (RealClock) NewTicker(d time.Duration) Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }
