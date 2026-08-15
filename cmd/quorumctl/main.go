// Command quorumctl is a CLI client for a Quorum cluster.
//
// Usage:
//
//	quorumctl -addrs 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 get <key>
//	quorumctl -addrs 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 set <key> <value>
//	quorumctl -addrs 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 delete <key>
//
// -addrs takes every node's client-facing address. The redirect-and-
// retry logic (follow a "not leader" LeaderHint, back off and rotate
// to a different node on a network error or "no leader" mid-election)
// lives in internal/client, shared with Phase 5's chaos harness so
// both exercise identical client behavior.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/client"
)

func main() {
	addrsFlag := flag.String("addrs", "127.0.0.1:8000", "comma-separated client-facing addresses of cluster nodes")
	timeout := flag.Duration("timeout", 3*time.Second, "per-request timeout")
	totalTimeout := flag.Duration("total-timeout", 10*time.Second, "overall time budget across all retries")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
		os.Exit(2)
	}
	addrs := strings.Split(*addrsFlag, ",")
	c := client.New(addrs, *timeout, *totalTimeout)

	switch args[0] {
	case "get":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		value, found, err := c.Get(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "quorumctl: error: %v\n", err)
			os.Exit(1)
		}
		if !found {
			fmt.Println("(not found)")
		} else {
			fmt.Println(value)
		}
	case "set":
		if len(args) != 3 {
			usage()
			os.Exit(2)
		}
		if err := c.Set(args[1], args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "quorumctl: error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")
	case "delete":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		if err := c.Delete(args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "quorumctl: error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  quorumctl [-addrs host:port,...] [-timeout d] [-total-timeout d] get <key>
  quorumctl [-addrs host:port,...] [-timeout d] [-total-timeout d] set <key> <value>
  quorumctl [-addrs host:port,...] [-timeout d] [-total-timeout d] delete <key>`)
}
