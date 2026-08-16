// Command chaos runs Quorum's Phase 5 chaos-testing scenarios against
// real quorumd processes over real TCP -- see internal/chaos for why
// that matters (a real wire-protocol bug was invisible to every
// FakeTransport-based test in Phases 2-3 and only surfaced under live
// verification; this harness exists to keep exercising that real
// path).
//
// Usage:
//
//	chaos leader-crash [-nodes N] [-rounds N] [-base-dir dir]
//
// Requires a built quorumd binary; pass its path with -quorumd-bin, or
// build one at ./bin/quorumd first (the Makefile's `chaos` target does
// this).
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/chaos"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	scenario := os.Args[1]
	fs := flag.NewFlagSet(scenario, flag.ExitOnError)
	nodes := fs.Int("nodes", 5, "cluster size")
	rounds := fs.Int("rounds", 5, "number of kill/recover rounds")
	writers := fs.Int("writers", 5, "(concurrent/sustained) number of concurrent writer goroutines")
	kills := fs.Int("kills", 3, "(concurrent only) number of leader kills to force during the run")
	duration := fs.Duration("duration", 5*time.Minute, "(sustained only) total run duration")
	seed := fs.Int64("seed", time.Now().UnixNano(), "(sustained only) RNG seed for fault timing/choice (logged at start; real timing is still non-deterministic)")
	baseDir := fs.String("base-dir", "", "directory for node data (default: a fresh temp dir)")
	binaryPath := fs.String("quorumd-bin", "./bin/quorumd", "path to a built quorumd binary")
	fs.Parse(os.Args[2:])

	dir := *baseDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "quorum-chaos-*")
		if err != nil {
			log.Fatalf("chaos: %v", err)
		}
	}
	if _, err := os.Stat(*binaryPath); err != nil {
		log.Fatalf("chaos: quorumd binary not found at %s (build it first, e.g. `go build -o bin/quorumd ./cmd/quorumd`): %v", *binaryPath, err)
	}

	switch scenario {
	case "leader-crash":
		cfg := chaos.DefaultLeaderCrashConfig(dir, *binaryPath)
		cfg.NumNodes = *nodes
		cfg.Rounds = *rounds
		runLeaderCrash(cfg)
	case "partition":
		cfg := chaos.DefaultPartitionConfig(dir, *binaryPath)
		cfg.NumNodes = *nodes
		runPartition(cfg)
	case "concurrent":
		cfg := chaos.DefaultConcurrentConfig(dir, *binaryPath)
		cfg.NumNodes = *nodes
		cfg.NumWriters = *writers
		cfg.Kills = *kills
		runConcurrent(cfg)
	case "sustained":
		cfg := chaos.DefaultSustainedConfig(dir, *binaryPath)
		cfg.NumNodes = *nodes
		cfg.NumWriters = *writers
		cfg.Duration = *duration
		cfg.Seed = *seed
		runSustained(cfg)
	default:
		usage()
		os.Exit(2)
	}
}

func runLeaderCrash(cfg chaos.LeaderCrashConfig) {
	fmt.Printf("=== Scenario 1: leader-crash-mid-write ===\n")
	fmt.Printf("nodes=%d rounds=%d election=%s-%s heartbeat=%s\n\n",
		cfg.NumNodes, cfg.Rounds, cfg.ElectionMin, cfg.ElectionMax, cfg.Heartbeat)

	start := time.Now()
	report, err := chaos.RunLeaderCrash(cfg)
	if err != nil {
		log.Fatalf("chaos: leader-crash scenario failed: %v", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("\n--- Results (total wall time %s) ---\n", elapsed)
	var min, max, sum time.Duration
	for i, r := range report.Rounds {
		fmt.Printf("round %d: killed %-4s -> new leader %-4s in %s\n", r.Round, r.KilledNode, r.NewLeader, r.ElectionDuration)
		if i == 0 || r.ElectionDuration < min {
			min = r.ElectionDuration
		}
		if r.ElectionDuration > max {
			max = r.ElectionDuration
		}
		sum += r.ElectionDuration
	}
	avg := sum / time.Duration(len(report.Rounds))
	fmt.Printf("\nelection time: min=%s max=%s avg=%s (n=%d)\n", min, max, avg, len(report.Rounds))
	fmt.Printf("writes: attempted=%d acked=%d\n", report.TotalWritesAttempted, report.TotalWritesAcked)

	if len(report.Violations) == 0 {
		fmt.Printf("verification: PASS -- every acknowledged write present with the correct value, no fabricated state\n")
	} else {
		fmt.Printf("verification: FAIL -- %d violation(s):\n", len(report.Violations))
		for _, v := range report.Violations {
			fmt.Printf("  [rule %d] %s\n", v.Rule, v.Message)
		}
		os.Exit(1)
	}
}

func runPartition(cfg chaos.PartitionConfig) {
	fmt.Printf("=== Scenario 2: network-partition / split-brain ===\n")
	fmt.Printf("nodes=%d minority-size=%d election=%s-%s heartbeat=%s settling-delay=%s\n\n",
		cfg.NumNodes, cfg.MinoritySize, cfg.ElectionMin, cfg.ElectionMax, cfg.Heartbeat, cfg.SettlingDelay)

	start := time.Now()
	report, err := chaos.RunPartition(cfg)
	if err != nil {
		log.Fatalf("chaos: partition scenario failed: %v", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("\n--- Results (total wall time %s) ---\n", elapsed)
	fmt.Printf("minority: %v\n", report.Minority)
	fmt.Printf("majority: %v\n", report.Majority)
	fmt.Printf("minority write probes: %d/%d correctly rejected\n", report.MinorityProbesBlocked, report.MinorityProbesTotal)
	fmt.Printf("majority writes while isolated: %d/%d acked\n", report.MajorityWritesAcked, report.MajorityWritesTotal)

	safe := report.MinorityProbesBlocked == report.MinorityProbesTotal
	if !safe {
		fmt.Printf("\nSAFETY: FAIL -- the minority partition accepted at least one write while isolated\n")
	} else {
		fmt.Printf("\nSAFETY: PASS -- the minority partition never accepted a write while isolated\n")
	}

	if len(report.Violations) == 0 {
		fmt.Printf("verification: PASS -- post-heal state matches every acknowledged write, no fabricated state\n")
	} else {
		fmt.Printf("verification: FAIL -- %d violation(s):\n", len(report.Violations))
		for _, v := range report.Violations {
			fmt.Printf("  [rule %d] %s\n", v.Rule, v.Message)
		}
	}
	if !safe || len(report.Violations) != 0 {
		os.Exit(1)
	}
}

func runConcurrent(cfg chaos.ConcurrentConfig) {
	fmt.Printf("=== Scenario 3: concurrent writes under failover ===\n")
	fmt.Printf("nodes=%d writers=%d kills=%d election=%s-%s heartbeat=%s\n\n",
		cfg.NumNodes, cfg.NumWriters, cfg.Kills, cfg.ElectionMin, cfg.ElectionMax, cfg.Heartbeat)

	start := time.Now()
	report, err := chaos.RunConcurrent(cfg)
	if err != nil {
		log.Fatalf("chaos: concurrent scenario failed: %v", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("\n--- Results (total wall time %s) ---\n", elapsed)
	for i, k := range report.Kills {
		fmt.Printf("kill %d: killed %-4s -> new leader %-4s in %s (writers kept running)\n", i+1, k.KilledNode, k.NewLeader, k.ElectionDuration)
	}
	fmt.Printf("\nwrites: attempted=%d acked=%d\n", report.TotalWritesAttempted, report.TotalWritesAcked)

	if len(report.Violations) == 0 {
		fmt.Printf("verification: PASS -- every acknowledged write (including every hot-key overwrite, checked for correct final ordering) present with the correct value, no fabricated state\n")
	} else {
		fmt.Printf("verification: FAIL -- %d violation(s):\n", len(report.Violations))
		for _, v := range report.Violations {
			fmt.Printf("  [rule %d] %s\n", v.Rule, v.Message)
		}
		os.Exit(1)
	}
}

func runSustained(cfg chaos.SustainedConfig) {
	fmt.Printf("=== Scenario 4: sustained fault injection ===\n")
	fmt.Printf("nodes=%d writers=%d duration=%s election=%s-%s heartbeat=%s fault-interval=%s-%s seed=%d\n\n",
		cfg.NumNodes, cfg.NumWriters, cfg.Duration, cfg.ElectionMin, cfg.ElectionMax, cfg.Heartbeat,
		cfg.FaultIntervalMin, cfg.FaultIntervalMax, cfg.Seed)

	start := time.Now()
	report, err := chaos.RunSustained(cfg)
	if err != nil {
		log.Fatalf("chaos: sustained scenario failed: %v", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("\n--- Fault log (%d events) ---\n", len(report.Faults))
	kills, partitions := 0, 0
	for i, f := range report.Faults {
		fmt.Printf("%2d. %-9s target=%-20s duration=%s\n", i+1, f.Kind, f.Target, f.Recovered.Sub(f.Start))
		if f.Kind == "kill" {
			kills++
		} else {
			partitions++
		}
	}

	fmt.Printf("\n--- Results (total wall time %s) ---\n", elapsed)
	fmt.Printf("faults: %d kills, %d partitions\n", kills, partitions)
	fmt.Printf("writes: attempted=%d acked=%d\n", report.WritesAttempted, report.WritesAcked)
	fmt.Printf("reads:  attempted=%d ok=%d expected-transient=%d mismatches=%d\n",
		report.ReadsAttempted, report.ReadsOK, report.ReadsExpectedTransient, len(report.Mismatches))

	persisted := 0
	if len(report.Mismatches) > 0 {
		fmt.Printf("\n--- Read mismatches (%d), with follow-up ---\n", len(report.Mismatches))
		for _, m := range report.Mismatches {
			status := "SELF-CORRECTED"
			if !m.SelfCorrected {
				status = "PERSISTED -- genuine issue"
				persisted++
			}
			fmt.Printf("  [%s] key=%q at %s: expected found=%v value=%q, got found=%v value=%q; follow-up found=%v value=%q\n",
				status, m.Key, m.At.Format(time.RFC3339Nano), m.ExpectedFound, m.ExpectedValue, m.GotFound, m.GotValue, m.FollowUpFound, m.FollowUpValue)
		}
	}

	if len(report.Violations) == 0 && persisted == 0 {
		fmt.Printf("\nverification: PASS -- zero data loss, zero persisted inconsistency across the full run\n")
	} else {
		fmt.Printf("\nverification: FAIL\n")
		if persisted > 0 {
			fmt.Printf("  %d read mismatch(es) never self-corrected -- see above, these are genuine and need investigation, not expected-transient\n", persisted)
		}
		for _, v := range report.Violations {
			fmt.Printf("  [rule %d] %s\n", v.Rule, v.Message)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: chaos <scenario> [flags]

scenarios:
  leader-crash   start a cluster, sustained writes, repeatedly kill -9 the leader
  partition      split into a majority/minority, prove the minority can't write, heal, verify
  concurrent     several concurrent writers, repeated failovers mid-run, verify no loss/corruption
  sustained      random kill/restart and partition/heal for -duration, continuous read/write, verify

flags:
  -nodes N          cluster size (default 5)
  -rounds N         (leader-crash only) number of kill/recover rounds (default 5)
  -writers N        (concurrent/sustained) number of concurrent writer goroutines (default 5)
  -kills N          (concurrent only) number of leader kills during the run (default 3)
  -duration d       (sustained only) total run duration (default 5m)
  -seed N           (sustained only) RNG seed for fault timing/choice
  -base-dir dir     node data directory (default: fresh temp dir)
  -quorumd-bin path path to a built quorumd binary (default ./bin/quorumd)`)
}
