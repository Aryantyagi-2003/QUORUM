package chaos

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/client"
)

// SustainedConfig configures scenario 4: random, unscripted fault
// injection (kill-and-restart or partition-and-heal, chosen and timed
// randomly, never overlapping -- always leaving a working majority)
// for the full Duration, while a continuous writer and a continuous
// reader run throughout.
type SustainedConfig struct {
	NumNodes                 int
	Duration                 time.Duration
	BaseDir, BinaryPath      string
	ElectionMin, ElectionMax time.Duration
	Heartbeat, RPCTimeout    time.Duration
	SettlingDelay            time.Duration
	NumWriters               int
	WriteInterval            time.Duration
	ReadInterval             time.Duration
	FaultIntervalMin         time.Duration // gap between the end of one fault's recovery and the start of considering the next
	FaultIntervalMax         time.Duration
	Seed                     int64
}

func DefaultSustainedConfig(baseDir, binaryPath string) SustainedConfig {
	return SustainedConfig{
		NumNodes:         5,
		Duration:         5 * time.Minute,
		BaseDir:          baseDir,
		BinaryPath:       binaryPath,
		ElectionMin:      150 * time.Millisecond,
		ElectionMax:      300 * time.Millisecond,
		Heartbeat:        50 * time.Millisecond,
		RPCTimeout:       2 * time.Second,
		SettlingDelay:    DefaultSettlingDelay,
		NumWriters:       3,
		WriteInterval:    50 * time.Millisecond,
		ReadInterval:     100 * time.Millisecond,
		FaultIntervalMin: 2 * time.Second,
		FaultIntervalMax: 6 * time.Second,
		Seed:             time.Now().UnixNano(),
	}
}

type FaultEvent struct {
	Kind      string // "kill" or "partition"
	Target    string // node ID (kill) or minority description (partition)
	Start     time.Time
	Recovered time.Time
}

type ReadMismatch struct {
	Key           string
	ExpectedValue string
	ExpectedFound bool
	GotValue      string
	GotFound      bool
	At            time.Time
	SelfCorrected bool
	FollowUpValue string
	FollowUpFound bool
}

type SustainedReport struct {
	Faults                 []FaultEvent
	WritesAttempted        int
	WritesAcked            int
	ReadsAttempted         int
	ReadsOK                int
	ReadsExpectedTransient int
	Mismatches             []ReadMismatch // read a clean, definite answer that disagreed with known state
	Violations             []Violation    // final full-state verification
}

// RunSustained executes scenario 4 end to end.
func RunSustained(cfg SustainedConfig) (*SustainedReport, error) {
	nodeIDs := make([]string, cfg.NumNodes)
	for i := range nodeIDs {
		nodeIDs[i] = fmt.Sprintf("n%d", i+1)
	}

	cluster, err := StandUp(ClusterConfig{
		NodeIDs:     nodeIDs,
		BaseDir:     cfg.BaseDir,
		BinaryPath:  cfg.BinaryPath,
		ElectionMin: cfg.ElectionMin,
		ElectionMax: cfg.ElectionMax,
		Heartbeat:   cfg.Heartbeat,
		RPCTimeout:  cfg.RPCTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("chaos: stand up cluster: %w", err)
	}
	defer cluster.Close()

	log.Printf("[chaos] cluster of %d nodes starting, waiting for initial leader... (seed=%d)", cfg.NumNodes, cfg.Seed)
	if _, ok := waitForLeader(cluster.ClientAddrs, cfg.RPCTimeout, 10*time.Second); !ok {
		return nil, fmt.Errorf("chaos: no initial leader elected within budget")
	}

	verifier := NewVerifier()
	var mu sync.Mutex // guards verifier, writtenKeys, mismatches, report accumulation
	var writtenKeys []string
	report := &SustainedReport{}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: continuous, unique keys, spread across NumWriters
	// goroutines so failover contention is realistic (not one
	// serialized stream).
	for w := 0; w < cfg.NumWriters; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := client.New(cluster.AllClientAddrs(), cfg.RPCTimeout, 4*time.Second)
			i := 0
			ticker := time.NewTicker(cfg.WriteInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					i++
					key := fmt.Sprintf("s-w%d-%d", idx, i)
					value := fmt.Sprintf("v%d-%d", idx, i)
					issued := time.Now()
					err := c.Set(key, value)
					mu.Lock()
					verifier.RecordWrite(WriteRecord{Key: key, Value: value, Seq: 1, Acked: err == nil, Issued: issued}, "")
					report.WritesAttempted++
					if err == nil {
						report.WritesAcked++
						writtenKeys = append(writtenKeys, key)
					}
					mu.Unlock()
				}
			}
		}(w)
	}

	// Reader: periodically re-reads a random previously-written key and
	// compares against the highest acked value the Verifier knows for
	// it. A clean, definite disagreement is recorded as a mismatch for
	// follow-up after the run -- see the package doc on why that's not
	// immediately treated as data loss. Any error (network failure,
	// "no leader", "not leader") is bucketed as expected-transient: the
	// client.Client API only ever returns an error from Get in exactly
	// those situations (see internal/client), never as a side channel
	// for "something is wrong."
	// rng drives the fault-injection loop below (kill-vs-partition
	// choice, targets, timings); readerRng is a SEPARATE instance for
	// the reader goroutine. math/rand.Rand is not safe for concurrent
	// use, and the fault injector and reader run concurrently -- a
	// shared *rand.Rand here would be a real data race, exactly the
	// kind -race exists to catch, so each gets its own.
	rng := rand.New(rand.NewSource(cfg.Seed))
	readerRng := rand.New(rand.NewSource(cfg.Seed + 1))
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := client.New(cluster.AllClientAddrs(), cfg.RPCTimeout, 4*time.Second)
		ticker := time.NewTicker(cfg.ReadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				mu.Lock()
				if len(writtenKeys) == 0 {
					mu.Unlock()
					continue
				}
				key := writtenKeys[readerRng.Intn(len(writtenKeys))]
				expected := latestAcked(verifier, key)
				mu.Unlock()
				if expected == nil {
					continue
				}

				got, found, err := c.Get(key)
				mu.Lock()
				report.ReadsAttempted++
				switch {
				case err != nil:
					report.ReadsExpectedTransient++
				case found == expected.found && (!found || got == expected.value):
					report.ReadsOK++
				default:
					report.Mismatches = append(report.Mismatches, ReadMismatch{
						Key: key, ExpectedValue: expected.value, ExpectedFound: expected.found,
						GotValue: got, GotFound: found, At: time.Now(),
					})
					log.Printf("[chaos] READ MISMATCH: key=%q expected found=%v value=%q, got found=%v value=%q -- recording for follow-up",
						key, expected.found, expected.value, found, got)
				}
				mu.Unlock()
			}
		}
	}()

	// Fault injector: random kind, random target, random duration,
	// never overlapping -- always fully recovers (restart / heal)
	// before considering the next fault, so at most one fault is ever
	// active and a working majority is always preserved.
	end := time.Now().Add(cfg.Duration)
	for time.Now().Before(end) {
		gap := cfg.FaultIntervalMin + time.Duration(rng.Int63n(int64(cfg.FaultIntervalMax-cfg.FaultIntervalMin+1)))
		time.Sleep(gap)
		if time.Now().After(end) {
			break
		}

		if rng.Intn(2) == 0 {
			injectKill(cluster, rng, &mu, report)
		} else {
			injectPartition(cluster, rng, cfg.SettlingDelay, &mu, report, verifier)
		}
	}

	log.Printf("[chaos] fault injection window done, stopping workload...")
	close(stop)
	wg.Wait()
	time.Sleep(cfg.SettlingDelay * 4)

	// Follow up on every recorded mismatch: does it still disagree, or
	// has it since resolved? This is what distinguishes a transient,
	// expected consistency gap (self-corrects once the serving node's
	// bookkeeping catches up) from a genuine, persistent loss.
	finalLeaderID, ok := CurrentLeader(cluster.ClientAddrs, cfg.RPCTimeout)
	if !ok {
		return nil, fmt.Errorf("chaos: no leader at final verification")
	}
	fc := client.New([]string{cluster.ClientAddrs[finalLeaderID]}, cfg.RPCTimeout, 5*time.Second)
	for i := range report.Mismatches {
		m := &report.Mismatches[i]
		value, found, err := fc.Get(m.Key)
		if err == nil && found == m.ExpectedFound && (!found || value == m.ExpectedValue) {
			m.SelfCorrected = true
		}
		m.FollowUpValue, m.FollowUpFound = value, found
	}

	// Full-state verification, same as scenarios 1-3.
	seen := make(map[string]bool)
	var final []FinalState
	for _, w := range verifier.Writes {
		if seen[w.Key] {
			continue
		}
		seen[w.Key] = true
		value, found, err := fc.Get(w.Key)
		if err != nil {
			return nil, fmt.Errorf("chaos: final get %s: %w", w.Key, err)
		}
		final = append(final, FinalState{Key: w.Key, Value: value, Found: found})
	}
	report.Violations = verifier.Verify(final)

	return report, nil
}

type ackedValue struct {
	value string
	found bool // false means the latest acked op was a Delete
}

// latestAcked returns the highest-Seq acked WriteRecord's value for
// key, or nil if none exists yet. Callers must hold the verifier's
// associated mutex.
func latestAcked(v *Verifier, key string) *ackedValue {
	var best *WriteRecord
	for i := range v.Writes {
		w := &v.Writes[i]
		if w.Key != key || !w.Acked {
			continue
		}
		if best == nil || w.Seq > best.Seq {
			best = w
		}
	}
	if best == nil {
		return nil
	}
	return &ackedValue{value: best.Value, found: true}
}

func injectKill(cluster *Cluster, rng *rand.Rand, mu *sync.Mutex, report *SustainedReport) {
	ids := make([]string, 0, len(cluster.Nodes))
	for id := range cluster.Nodes {
		ids = append(ids, id)
	}
	target := ids[rng.Intn(len(ids))]

	log.Printf("[chaos] fault: killing %s", target)
	start := time.Now()
	if err := cluster.Nodes[target].Kill(); err != nil {
		log.Printf("[chaos] kill %s: %v", target, err)
	}

	recoveryDelay := 300*time.Millisecond + time.Duration(rng.Int63n(int64(1200*time.Millisecond)))
	time.Sleep(recoveryDelay)

	if err := cluster.Nodes[target].Start(); err != nil {
		log.Printf("[chaos] restart %s failed: %v", target, err)
	}
	recovered := time.Now()
	log.Printf("[chaos] fault: %s restarted after %s", target, recovered.Sub(start))

	mu.Lock()
	report.Faults = append(report.Faults, FaultEvent{Kind: "kill", Target: target, Start: start, Recovered: recovered})
	mu.Unlock()
}

func injectPartition(cluster *Cluster, rng *rand.Rand, settlingDelay time.Duration, mu *sync.Mutex, report *SustainedReport, verifier *Verifier) {
	ids := make([]string, 0, len(cluster.Nodes))
	for id := range cluster.Nodes {
		ids = append(ids, id)
	}
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

	minoritySize := 1 + rng.Intn(2) // 1 or 2, always < majority for a 5-node cluster
	minority := append([]string{}, ids[:minoritySize]...)
	majority := append([]string{}, ids[minoritySize:]...)

	log.Printf("[chaos] fault: partitioning minority=%v", minority)
	cluster.Net.Partition(minority, majority)
	start := time.Now()
	windowStart := start.Add(settlingDelay)
	time.Sleep(settlingDelay)

	isolatedDuration := 500*time.Millisecond + time.Duration(rng.Int63n(int64(1500*time.Millisecond)))
	time.Sleep(isolatedDuration)

	cluster.Net.HealAll()
	windowEnd := time.Now()
	log.Printf("[chaos] fault: healed partition of %v after %s isolated", minority, windowEnd.Sub(start))

	mu.Lock()
	report.Faults = append(report.Faults, FaultEvent{
		Kind: "partition", Target: fmt.Sprintf("%v", minority), Start: start, Recovered: windowEnd,
	})
	verifier.PartitionWindows = append(verifier.PartitionWindows, PartitionWindow{
		Minority: minority, Start: windowStart, End: windowEnd,
	})
	mu.Unlock()
}

// isTransientError is unused directly (Get's error contract already
// guarantees every error is transient-by-construction -- see the
// reader goroutine's comment) but kept here as executable documentation
// of what "transient" means for this project, for anyone auditing the
// classification later.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{"no leader", "not leader", "timeout", "dial", "connection refused", "EOF", "i/o timeout"} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}
