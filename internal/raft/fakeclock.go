package raft

import (
	"sync"
	"time"
)

// FakeClock is a manually-advanced Clock for deterministic tests. All
// exported methods are safe to call from a test goroutine while Core
// runs on its own goroutine.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*fakeWaiter
}

type fakeWaiter struct {
	deadline time.Time
	period   time.Duration // 0 for a one-shot Timer, >0 for a repeating Ticker
	ch       chan time.Time
	active   bool // false after a Timer fires (until Reset) or after Stop
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWaiter{deadline: c.now.Add(d), ch: make(chan time.Time), active: true}
	c.waiters = append(c.waiters, w)
	return &fakeTimer{clock: c, w: w}
}

func (c *FakeClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWaiter{deadline: c.now.Add(d), period: d, ch: make(chan time.Time), active: true}
	c.waiters = append(c.waiters, w)
	return &fakeTicker{clock: c, w: w}
}

// AdvanceAndSync moves the fake clock's virtual time forward by d,
// firing every timer/ticker whose deadline falls at-or-before the new
// time, strictly in deadline order. Each fire is delivered via a
// blocking send on that timer's channel, so AdvanceAndSync does not
// return until every timer/ticker fire it triggers has actually been
// received by its consumer (e.g. Core's select loop) — that's the
// "Sync" in the name, and it is the ONLY thing this method
// synchronizes on.
//
// It deliberately does NOT wait for the consumer to finish reacting to
// a fire (e.g. Core running its startElection logic, sending RPCs, or
// changing role) — only for the timer signal itself to be handed off.
// If a test needs to observe some effect of that reaction, that's a
// separate, explicit wait belonging to the test — for example
// Core.State() is itself a synchronous round trip through Core's own
// channel, and is correctly ordered after whatever case body Core was
// running when it received the fire, simply because Core's select loop
// cannot reach a second select statement until the first case's body
// has returned. AdvanceAndSync does not need to know or care about
// that; it stays a single-purpose timer-delivery primitive, not a
// general "Core has finished processing everything" barrier.
func (c *FakeClock) AdvanceAndSync(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	for {
		w := c.nextDueWaiterLocked(target)
		if w == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		c.now = w.deadline
		fired := w.deadline
		if w.period > 0 {
			w.deadline = w.deadline.Add(w.period) // ticker: reschedule, stays active
		} else {
			w.active = false // timer: one-shot, needs Reset to fire again
		}
		c.mu.Unlock()
		w.ch <- fired // blocking send: waits for the consumer to receive it
		c.mu.Lock()
	}
}

// nextDueWaiterLocked returns the active waiter with the earliest
// deadline at-or-before target, or nil if none. Callers must hold c.mu.
func (c *FakeClock) nextDueWaiterLocked(target time.Time) *fakeWaiter {
	var best *fakeWaiter
	for _, w := range c.waiters {
		if !w.active || w.deadline.After(target) {
			continue
		}
		if best == nil || w.deadline.Before(best.deadline) {
			best = w
		}
	}
	return best
}

type fakeTimer struct {
	clock *FakeClock
	w     *fakeWaiter
}

func (t *fakeTimer) C() <-chan time.Time { return t.w.ch }

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.w.active
	t.w.deadline = t.clock.now.Add(d)
	t.w.active = true
	return wasActive
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.w.active
	t.w.active = false
	return wasActive
}

type fakeTicker struct {
	clock *FakeClock
	w     *fakeWaiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }

func (t *fakeTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.w.active = false
}
