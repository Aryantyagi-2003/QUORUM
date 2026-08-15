package chaos

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/client"
)

// ConcurrentConfig configures scenario 3: several concurrent clients
// writing while the harness forces repeated leader failovers mid-run,
// then a programmatic (not manual) check that every acknowledged
// write survived and nothing was lost or corrupted.
//
// Each writer does two kinds of writes, deliberately: most writes go
// to a unique key (never repeated), proving ordinary concurrent-write
// correctness under failover load. But each writer ALSO repeatedly
// overwrites its own single "hot key" with an increasing sequence
// number embedded in the value. That's the part specifically checking
// what the leader-kill-mid-write retry path guarantees: if the wrong
// (stale) write ever "won" the hot key -- an ack for seq N arriving,
// committing, then being silently overtaken by a late-arriving seq
// N-1 -- the Verifier's existing rule 1 (highest acked Seq per key
// must match final state) already catches it. No new verifier logic
// needed; the workload shape is what makes rule 1 exercise this
// property, since scenarios 1 and 2 only ever wrote each key once.
type ConcurrentConfig struct {
	NumNodes              int
	NumWriters            int
	Kills                 int
	KillInterval          time.Duration
	UniqueWritesPerWriter int
	HotKeyWritesPerWriter int
	BaseDir, BinaryPath   string
	ElectionMin           time.Duration
	ElectionMax           time.Duration
	Heartbeat             time.Duration
	RPCTimeout            time.Duration
	SettleAfter           time.Duration
}

func DefaultConcurrentConfig(baseDir, binaryPath string) ConcurrentConfig {
	return ConcurrentConfig{
		NumNodes:              5,
		NumWriters:            5,
		Kills:                 3,
		KillInterval:          800 * time.Millisecond,
		UniqueWritesPerWriter: 30,
		HotKeyWritesPerWriter: 30,
		BaseDir:               baseDir,
		BinaryPath:            binaryPath,
		ElectionMin:           150 * time.Millisecond,
		ElectionMax:           300 * time.Millisecond,
		Heartbeat:             50 * time.Millisecond,
		RPCTimeout:            2 * time.Second,
		SettleAfter:           1 * time.Second,
	}
}

type KillEvent struct {
	KilledNode       string
	NewLeader        string
	ElectionDuration time.Duration
}

type ConcurrentReport struct {
	Kills                []KillEvent
	TotalWritesAttempted int
	TotalWritesAcked     int
	Violations           []Violation
}

// RunConcurrent executes scenario 3 end to end.
func RunConcurrent(cfg ConcurrentConfig) (*ConcurrentReport, error) {
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

	log.Printf("[chaos] cluster of %d nodes starting, waiting for initial leader...", cfg.NumNodes)
	if _, ok := waitForLeader(cluster.ClientAddrs, cfg.RPCTimeout, 10*time.Second); !ok {
		return nil, fmt.Errorf("chaos: no initial leader elected within budget")
	}

	verifier := NewVerifier()
	var verifierMu sync.Mutex
	var writersWG sync.WaitGroup

	for w := 0; w < cfg.NumWriters; w++ {
		writersWG.Add(1)
		go func(writerIdx int) {
			defer writersWG.Done()
			c := client.New(cluster.AllClientAddrs(), cfg.RPCTimeout, 5*time.Second)
			hotKey := fmt.Sprintf("hot-%d", writerIdx)

			steps := cfg.UniqueWritesPerWriter + cfg.HotKeyWritesPerWriter
			hotSeq := uint64(0)
			for step := 0; step < steps; step++ {
				var key, value string
				var seq uint64
				if step%2 == 0 && step/2 < cfg.UniqueWritesPerWriter {
					key = fmt.Sprintf("w%d-u%d", writerIdx, step)
					value = fmt.Sprintf("uv%d", step)
					seq = 1
				} else {
					hotSeq++
					key = hotKey
					value = fmt.Sprintf("seq-%d", hotSeq)
					seq = hotSeq
				}
				issued := time.Now()
				err := c.Set(key, value)
				verifierMu.Lock()
				verifier.RecordWrite(WriteRecord{Key: key, Value: value, Seq: seq, Acked: err == nil, Issued: issued}, "")
				verifierMu.Unlock()
				time.Sleep(15 * time.Millisecond)
			}
		}(w)
	}

	report := &ConcurrentReport{}
	for k := 0; k < cfg.Kills; k++ {
		time.Sleep(cfg.KillInterval)

		leaderID, ok := CurrentLeader(cluster.ClientAddrs, cfg.RPCTimeout)
		if !ok {
			log.Printf("[chaos] kill %d: no leader found right now, skipping this kill", k+1)
			continue
		}
		log.Printf("[chaos] kill %d: current leader is %s -- killing (SIGKILL), writers keep running", k+1, leaderID)
		killTime := time.Now()
		if err := cluster.Nodes[leaderID].Kill(); err != nil {
			log.Printf("[chaos] kill %d: kill %s: %v", k+1, leaderID, err)
		}

		newLeaderID, ok := waitForLeaderExcluding(cluster.ClientAddrs, leaderID, cfg.RPCTimeout, 10*time.Second)
		if !ok {
			return nil, fmt.Errorf("kill %d: no new leader elected within budget after killing %s", k+1, leaderID)
		}
		electionDuration := time.Since(killTime)
		log.Printf("[chaos] kill %d: new leader %s elected in %s", k+1, newLeaderID, electionDuration)
		report.Kills = append(report.Kills, KillEvent{KilledNode: leaderID, NewLeader: newLeaderID, ElectionDuration: electionDuration})

		if err := cluster.Nodes[leaderID].Start(); err != nil {
			return nil, fmt.Errorf("kill %d: restart %s: %w", k+1, leaderID, err)
		}
	}

	log.Printf("[chaos] all kills done, waiting for writers to finish...")
	writersWG.Wait()
	time.Sleep(cfg.SettleAfter)

	finalLeaderID, ok := CurrentLeader(cluster.ClientAddrs, cfg.RPCTimeout)
	if !ok {
		return nil, fmt.Errorf("chaos: no leader at final verification")
	}
	log.Printf("[chaos] writers stopped, verifying against leader %s", finalLeaderID)

	fc := client.New([]string{cluster.ClientAddrs[finalLeaderID]}, cfg.RPCTimeout, 5*time.Second)
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
	for _, w := range verifier.Writes {
		report.TotalWritesAttempted++
		if w.Acked {
			report.TotalWritesAcked++
		}
	}
	return report, nil
}
