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

func usage() {
	fmt.Fprintln(os.Stderr, `usage: chaos <scenario> [flags]

scenarios:
  leader-crash   start a cluster, sustained writes, repeatedly kill -9 the leader

flags (leader-crash):
  -nodes N          cluster size (default 5)
  -rounds N         number of kill/recover rounds (default 5)
  -base-dir dir     node data directory (default: fresh temp dir)
  -quorumd-bin path path to a built quorumd binary (default ./bin/quorumd)`)
}
