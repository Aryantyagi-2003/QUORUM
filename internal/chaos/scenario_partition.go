package chaos

import (
	"fmt"
	"log"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/client"
	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

// PartitionConfig configures scenario 2: split a cluster into a
// majority and a minority, and prove -- as a hard assertion, not a
// soft check -- that the minority can never elect a leader or accept a
// write while isolated, while the majority continues operating
// normally. Then heal and confirm reconciliation without corruption.
type PartitionConfig struct {
	NumNodes       int // must be odd and >= 3 for a clean majority/minority split
	MinoritySize   int
	BaseDir        string
	BinaryPath     string
	ElectionMin    time.Duration
	ElectionMax    time.Duration
	Heartbeat      time.Duration
	RPCTimeout     time.Duration
	SettlingDelay  time.Duration // see verify.go's DefaultSettlingDelay doc for why this exists
	ProbesPerNode  int           // how many write attempts to make against each minority node while isolated
	MajorityWrites int           // how many writes to push through the majority while the minority is isolated
}

func DefaultPartitionConfig(baseDir, binaryPath string) PartitionConfig {
	return PartitionConfig{
		NumNodes:       5,
		MinoritySize:   2,
		BaseDir:        baseDir,
		BinaryPath:     binaryPath,
		ElectionMin:    150 * time.Millisecond,
		ElectionMax:    300 * time.Millisecond,
		Heartbeat:      50 * time.Millisecond,
		RPCTimeout:     2 * time.Second,
		SettlingDelay:  DefaultSettlingDelay,
		ProbesPerNode:  3,
		MajorityWrites: 10,
	}
}

type PartitionReport struct {
	Minority, Majority    []string
	MinorityProbesBlocked int // count of probe writes correctly rejected
	MinorityProbesTotal   int
	MajorityWritesAcked   int
	MajorityWritesTotal   int
	ReconciliationOK      bool
	Violations            []Violation
}

// RunPartition executes scenario 2 end to end.
func RunPartition(cfg PartitionConfig) (*PartitionReport, error) {
	if cfg.MinoritySize*2 >= cfg.NumNodes {
		return nil, fmt.Errorf("chaos: minority size %d is not a true minority of %d nodes", cfg.MinoritySize, cfg.NumNodes)
	}

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
	leaderID, ok := waitForLeader(cluster.ClientAddrs, cfg.RPCTimeout, 10*time.Second)
	if !ok {
		return nil, fmt.Errorf("chaos: no initial leader elected within budget")
	}
	log.Printf("[chaos] initial leader: %s", leaderID)

	// Put the current leader IN the minority -- the more dramatic,
	// classic split-brain shape (an old leader isolated with too few
	// followers to keep leading) than an arbitrary split.
	minority := []string{leaderID}
	majority := make([]string, 0, cfg.NumNodes-cfg.MinoritySize)
	for _, id := range nodeIDs {
		if id == leaderID {
			continue
		}
		if len(minority) < cfg.MinoritySize {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	log.Printf("[chaos] partitioning: minority=%v majority=%v", minority, majority)

	verifier := NewVerifier()
	report := &PartitionReport{Minority: minority, Majority: majority}

	cluster.Net.Partition(minority, majority)
	partitionCalledAt := time.Now()
	windowStart := partitionCalledAt.Add(cfg.SettlingDelay)
	// See verify.go's DefaultSettlingDelay doc: Partition() itself is
	// effectively instantaneous, but nothing synchronizes it with any
	// in-flight request, and a proxy only checks its drop flag at
	// Accept time, so a connection already established just before
	// Partition() can still legitimately finish. Sleeping past that
	// window before issuing (or asserting on) anything keeps this
	// scenario's own probes -- and the verifier's rule 3 -- from ever
	// being in that grey zone to begin with.
	time.Sleep(cfg.SettlingDelay)

	log.Printf("[chaos] settled; probing minority nodes directly (must all be rejected)...")
	// The isolated node that was leader before the partition never
	// demotes itself -- it never hears a higher term, since it can't
	// hear anything from the majority at all (confirmed behavior from
	// Phase 2/3's own tests) -- so it will still locally accept
	// Propose() and only fail via the server's internal
	// wait-for-applied timeout (2s, internal/server's
	// defaultWriteTimeout) once it can never reach majority to commit,
	// not a clean "not leader" response. The probe's own timeout must
	// comfortably exceed that internal timeout, or the client-side
	// deadline could race it and return a raw connection error instead
	// of a clean response -- still correctly counted as "not acked"
	// either way, but noisier to read.
	const probeTimeout = 4 * time.Second
	for _, id := range minority {
		addr := cluster.ClientAddrs[id]
		// Unique ClientID per node and an incrementing SeqNum per probe:
		// the server dedups by (ClientID, SeqNum) regardless of key, so
		// reusing either across probes could mask a real violation on a
		// later probe by silently no-op'ing it as a "duplicate" of an
		// earlier one -- weakening exactly the check this loop exists
		// to make.
		clientID := fmt.Sprintf("chaos-partition-probe-%s", id)
		for i := 0; i < cfg.ProbesPerNode; i++ {
			key := fmt.Sprintf("probe-%s-%d", id, i)
			value := "should-never-be-accepted"
			seq := uint64(i + 1)
			issued := time.Now()
			req := proto.ClientRequest{RPC: "Set", Key: key, Value: value, ClientID: clientID, SeqNum: seq}
			resp, err := RawRequest(addr, req, probeTimeout)
			acked := err == nil && resp.OK
			verifier.RecordWrite(WriteRecord{Key: key, Value: value, Seq: seq, Acked: acked, Issued: issued}, id)
			report.MinorityProbesTotal++
			if acked {
				log.Printf("[chaos] SAFETY VIOLATION: minority node %s ACCEPTED write %q while isolated (err=%v resp=%+v)", id, key, err, resp)
			} else {
				report.MinorityProbesBlocked++
				log.Printf("[chaos] minority node %s correctly rejected %q (err=%v, resp.Error=%q)", id, key, err, resp.Error)
			}
		}
	}

	log.Printf("[chaos] confirming majority continues accepting writes normally...")
	majorityAddrs := make([]string, len(majority))
	for i, id := range majority {
		majorityAddrs[i] = cluster.ClientAddrs[id]
	}
	mc := client.New(majorityAddrs, cfg.RPCTimeout, 5*time.Second)
	for i := 0; i < cfg.MajorityWrites; i++ {
		key := fmt.Sprintf("majority-%d", i)
		value := fmt.Sprintf("v%d", i)
		issued := time.Now()
		err := mc.Set(key, value)
		verifier.RecordWrite(WriteRecord{Key: key, Value: value, Seq: 1, Acked: err == nil, Issued: issued}, "")
		report.MajorityWritesTotal++
		if err == nil {
			report.MajorityWritesAcked++
		} else {
			log.Printf("[chaos] majority write %q failed unexpectedly: %v", key, err)
		}
	}
	log.Printf("[chaos] majority accepted %d/%d writes while minority was isolated", report.MajorityWritesAcked, report.MajorityWritesTotal)

	cluster.Net.HealAll()
	windowEnd := time.Now()
	log.Printf("[chaos] healed partition, waiting for reconciliation...")
	verifier.PartitionWindows = append(verifier.PartitionWindows, PartitionWindow{
		Minority: minority, Start: windowStart, End: windowEnd,
	})

	time.Sleep(cfg.SettlingDelay * 4) // give the healed minority time to catch up via AppendEntries before reading final state

	finalLeaderID, ok := CurrentLeader(cluster.ClientAddrs, cfg.RPCTimeout)
	if !ok {
		return nil, fmt.Errorf("chaos: no leader after healing")
	}
	log.Printf("[chaos] post-heal leader: %s, verifying...", finalLeaderID)

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
	report.ReconciliationOK = len(report.Violations) == 0 && report.MinorityProbesBlocked == report.MinorityProbesTotal
	return report, nil
}
