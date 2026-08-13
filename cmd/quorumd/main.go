// Command quorumd runs a Quorum node.
//
// Phase 1: single-node only, no Raft consensus — see internal/server
// doc comment. Multi-node flags (peer list, node ID) will be added
// once leader election lands.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/Aryantyagi-2003/Quorum/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "client-facing listen address")
	dataDir := flag.String("data-dir", "./data", "directory for durable state (hardstate.json, log.dat)")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("quorumd: create data dir: %v", err)
	}

	srv, err := server.Open(*addr, *dataDir)
	if err != nil {
		log.Fatalf("quorumd: %v", err)
	}
	defer srv.Close()

	if err := srv.Listen(); err != nil {
		log.Fatalf("quorumd: %v", err)
	}
}
