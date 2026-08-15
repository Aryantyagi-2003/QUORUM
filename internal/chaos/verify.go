package chaos

import (
	"fmt"
	"time"
)

// DefaultSettlingDelay is added after a partition event's Partition()
// call returns before the verifier will apply the minority's
// hard-zero-acks assertion (rule 3) to any write. See the package-level
// design note in isolatedWindow for why this exists: Partition() itself
// is effectively instantaneous (just atomic flag-sets), but nothing
// synchronizes it with a concurrently-running write workload, and a
// proxy only checks its drop flag at Accept time -- so a write already
// mid-flight when Partition() is called can legitimately complete a
// few milliseconds later. Without this delay, such a write would be
// misattributed as "the minority accepted a write during isolation."
// 250ms is generous relative to realistic localhost RPC latency
// (single-digit ms) and small relative to scenario durations (seconds
// to minutes).
const DefaultSettlingDelay = 250 * time.Millisecond

// WriteRecord is one client write attempt, as observed by the harness.
type WriteRecord struct {
	Key, Value string
	Seq        uint64
	Acked      bool // true only if the client actually received OK
	Issued     time.Time
}

// PartitionWindow marks one partition event's isolated span, for rule
// 3. Start is set AFTER Partition() returns, plus DefaultSettlingDelay
// -- not at the moment Partition() is called -- specifically to
// exclude the grey-zone writes described above. End is set at the
// moment Heal()/HealAll() is called for that event (no settling delay
// needed on that edge for rule 3's purposes: a write can't retroactively
// become "during isolation" just because healing is about to happen).
type PartitionWindow struct {
	Minority   []string
	Start, End time.Time
}

// contains reports whether t falls within [Start, End).
func (w PartitionWindow) contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

func inMinority(nodeID string, w PartitionWindow) bool {
	for _, id := range w.Minority {
		if id == nodeID {
			return true
		}
	}
	return false
}

// Verifier checks a chaos scenario's outcome against three rules:
//
//  1. For every key with at least one Acked write, the highest-Seq
//     acked value must match final cluster state. No claim is made
//     about unacked writes either way -- Raft itself makes none.
//  2. No key may appear in final cluster state with zero corresponding
//     write records at all (fabricated data).
//  3. For any WriteRecord attributed to a PartitionWindow's isolated
//     span (Issued time inside [Start, End)) and addressed to a node
//     in that window's minority, Acked must be false -- a hard
//     assertion, not a soft check.
type Verifier struct {
	Writes           []WriteRecord
	PartitionWindows []PartitionWindow
	writeNode        map[int]string // index into Writes -> which node ID the write was sent to (for rule 3); populated by RecordWrite
}

func NewVerifier() *Verifier {
	return &Verifier{writeNode: make(map[int]string)}
}

// RecordWrite logs one write attempt and, if it was addressed to a
// specific node (as opposed to going through redirect-and-retry to an
// unknown eventual leader), which node ID it was sent to -- needed for
// rule 3, which is specifically about writes issued to a minority
// node during its isolation.
func (v *Verifier) RecordWrite(rec WriteRecord, targetNodeID string) {
	v.writeNode[len(v.Writes)] = targetNodeID
	v.Writes = append(v.Writes, rec)
}

// FinalState is the final Get result for a key, from an authoritative
// (current-leader) read.
type FinalState struct {
	Key   string
	Value string
	Found bool
}

// Violation describes one failed rule.
type Violation struct {
	Rule    int
	Message string
}

// Verify applies all three rules and returns every violation found (nil
// if the scenario was clean).
func (v *Verifier) Verify(final []FinalState) []Violation {
	var violations []Violation

	// Rule 1 + 2: build the highest-acked-seq value per key, and the
	// set of keys with ANY record at all.
	bestAcked := make(map[string]WriteRecord)
	anyRecord := make(map[string]bool)
	for _, w := range v.Writes {
		anyRecord[w.Key] = true
		if !w.Acked {
			continue
		}
		if cur, ok := bestAcked[w.Key]; !ok || w.Seq > cur.Seq {
			bestAcked[w.Key] = w
		}
	}

	for _, f := range final {
		if !anyRecord[f.Key] {
			if f.Found {
				violations = append(violations, Violation{Rule: 2,
					Message: fmt.Sprintf("key %q present in cluster state with value %q but no write record exists for it at all", f.Key, f.Value)})
			}
			continue
		}
		want, hasAcked := bestAcked[f.Key]
		if !hasAcked {
			continue // only unacked writes for this key: no claim made either way (rule 1's asymmetry)
		}
		if !f.Found {
			violations = append(violations, Violation{Rule: 1,
				Message: fmt.Sprintf("key %q: last acked write set value %q (seq %d), but final state has no value", f.Key, want.Value, want.Seq)})
			continue
		}
		if f.Value != want.Value {
			violations = append(violations, Violation{Rule: 1,
				Message: fmt.Sprintf("key %q: last acked write set value %q (seq %d), but final state is %q", f.Key, want.Value, want.Seq, f.Value)})
		}
	}

	// Rule 3: any acked write issued to a minority node during ITS
	// isolated window (after settling) is a hard safety violation.
	for i, w := range v.Writes {
		if !w.Acked {
			continue
		}
		target := v.writeNode[i]
		for _, win := range v.PartitionWindows {
			if win.contains(w.Issued) && inMinority(target, win) {
				violations = append(violations, Violation{Rule: 3,
					Message: fmt.Sprintf("key %q (seq %d) was ACKED by minority node %q at %s, inside isolated window [%s, %s) -- safety violation",
						w.Key, w.Seq, target, w.Issued.Format(time.RFC3339Nano), win.Start.Format(time.RFC3339Nano), win.End.Format(time.RFC3339Nano))})
			}
		}
	}

	return violations
}
