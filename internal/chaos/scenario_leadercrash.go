package chaos

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/client"
)

// LeaderCrashConfig configures scenario 1: start a cluster, run a
// sustained write workload, and repeatedly kill -9 whoever the current
// leader is, measuring real election time and confirming no
// acknowledged write is ever lost.
type LeaderCrashConfig struct {
	NumNodes         int
	Rounds           int
	BaseDir          string
	BinaryPath       string
	ElectionMin      time.Duration
	ElectionMax      time.Duration
	Heartbeat        time.Duration
	RPCTimeout       time.Duration
	WriteInterval    time.Duration // how often the workload issues a new write
	SettleAfterRound time.Duration // pause after confirming a new leader before the next round / before final verification
}

func DefaultLeaderCrashConfig(baseDir, binaryPath string) LeaderCrashConfig {
	return LeaderCrashConfig{
		NumNodes:         5,
		Rounds:           5,
		BaseDir:          baseDir,
		BinaryPath:       binaryPath,
		ElectionMin:      150 * time.Millisecond, // the project's real, unmodified defaults
		ElectionMax:      300 * time.Millisecond,
		Heartbeat:        50 * time.Millisecond,
		RPCTimeout:       2 * time.Second,
		WriteInterval:    100 * time.Millisecond,
		SettleAfterRound: 1 * time.Second,
	}
}

type RoundResult struct {
	Round            int
	KilledNode       string
	NewLeader        string
	ElectionDuration time.Duration
}

type LeaderCrashReport struct {
	Rounds               []RoundResult
	TotalWritesAttempted int
	TotalWritesAcked     int
	Violations           []Violation
}

// RunLeaderCrash executes scenario 1 end to end and returns a report.
// Logs progress as it goes (via the standard log package) so a caller
// piping this to a file captures real, timestamped evidence -- not
// just the final summary.
func RunLeaderCrash(cfg LeaderCrashConfig) (*LeaderCrashReport, error) {
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
	stopWorkload := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c := client.New(cluster.AllClientAddrs(), cfg.RPCTimeout, 3*time.Second)
		ticker := time.NewTicker(cfg.WriteInterval)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-stopWorkload:
				return
			case <-ticker.C:
				i++
				key := fmt.Sprintf("k%d", i)
				value := fmt.Sprintf("v%d", i)
				issued := time.Now()
				err := c.Set(key, value)
				verifierMu.Lock()
				verifier.RecordWrite(WriteRecord{Key: key, Value: value, Seq: uint64(i), Acked: err == nil, Issued: issued}, "")
				verifierMu.Unlock()
			}
		}
	}()

	report := &LeaderCrashReport{}
	for round := 1; round <= cfg.Rounds; round++ {
		leaderID, ok := CurrentLeader(cluster.ClientAddrs, cfg.RPCTimeout)
		if !ok {
			close(stopWorkload)
			wg.Wait()
			return nil, fmt.Errorf("round %d: no leader found before kill", round)
		}
		log.Printf("[chaos] round %d: current leader is %s -- killing (SIGKILL)", round, leaderID)
		killTime := time.Now()
		if err := cluster.Nodes[leaderID].Kill(); err != nil {
			log.Printf("[chaos] round %d: kill %s: %v", round, leaderID, err)
		}

		newLeaderID, ok := waitForLeaderExcluding(cluster.ClientAddrs, leaderID, cfg.RPCTimeout, 10*time.Second)
		if !ok {
			close(stopWorkload)
			wg.Wait()
			return nil, fmt.Errorf("round %d: no new leader elected within budget after killing %s", round, leaderID)
		}
		electionDuration := time.Since(killTime)
		log.Printf("[chaos] round %d: new leader %s elected in %s", round, newLeaderID, electionDuration)

		report.Rounds = append(report.Rounds, RoundResult{
			Round: round, KilledNode: leaderID, NewLeader: newLeaderID, ElectionDuration: electionDuration,
		})

		// Restart the killed node from its same data dir -- proves
		// recovery from durable state, and keeps the cluster at full
		// strength for the next round.
		if err := cluster.Nodes[leaderID].Start(); err != nil {
			close(stopWorkload)
			wg.Wait()
			return nil, fmt.Errorf("round %d: restart %s: %w", round, leaderID, err)
		}
		time.Sleep(cfg.SettleAfterRound)
	}

	close(stopWorkload)
	wg.Wait()
	time.Sleep(cfg.SettleAfterRound) // let the last few writes finish applying everywhere

	finalLeaderID, ok := CurrentLeader(cluster.ClientAddrs, cfg.RPCTimeout)
	if !ok {
		return nil, fmt.Errorf("chaos: no leader at final verification")
	}
	log.Printf("[chaos] workload stopped, verifying against leader %s", finalLeaderID)

	fc := client.New([]string{cluster.ClientAddrs[finalLeaderID]}, cfg.RPCTimeout, 5*time.Second)
	var final []FinalState
	for _, w := range verifier.Writes {
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

func waitForLeader(clientAddrs map[string]string, rpcTimeout, budget time.Duration) (string, bool) {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if id, ok := CurrentLeader(clientAddrs, rpcTimeout); ok {
			return id, true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", false
}

func waitForLeaderExcluding(clientAddrs map[string]string, exclude string, rpcTimeout, budget time.Duration) (string, bool) {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if id, ok := CurrentLeader(clientAddrs, rpcTimeout); ok && id != exclude {
			return id, true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", false
}
