// Command quorumctl is a CLI client for a Quorum cluster.
//
// Usage:
//
//	quorumctl -addrs 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 get <key>
//	quorumctl -addrs 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 set <key> <value>
//	quorumctl -addrs 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 delete <key>
//
// Phase 4: -addrs takes every node's client-facing address. quorumctl
// tries one, and if it isn't the leader, follows the LeaderHint it
// returns; if a node is unreachable or reports "no leader" (mid-
// election), it backs off briefly and tries a different node. The same
// ClientID/SeqNum is reused across every retry within one invocation,
// so if an earlier attempt's write actually succeeded but the
// acknowledgment was lost (e.g. the leader crashed right after
// committing), a retried write is a safe no-op on the server side
// rather than being applied twice.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
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

	req := proto.ClientRequest{
		ClientID: fmt.Sprintf("quorumctl-%d", rand.Int63()),
		SeqNum:   1,
	}
	switch args[0] {
	case "get":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		req.RPC = "Get"
		req.Key = args[1]
	case "set":
		if len(args) != 3 {
			usage()
			os.Exit(2)
		}
		req.RPC = "Set"
		req.Key = args[1]
		req.Value = args[2]
	case "delete":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		req.RPC = "Delete"
		req.Key = args[1]
	default:
		usage()
		os.Exit(2)
	}

	resp, err := sendWithRetry(addrs, *timeout, *totalTimeout, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "quorumctl: %v\n", err)
		os.Exit(1)
	}
	if !resp.OK {
		fmt.Fprintf(os.Stderr, "quorumctl: error: %s\n", resp.Error)
		os.Exit(1)
	}

	switch req.RPC {
	case "Get":
		if !resp.Found {
			fmt.Println("(not found)")
		} else {
			fmt.Println(resp.Value)
		}
	default:
		fmt.Println("OK")
	}
}

// sendWithRetry tries addrs[0] first. A "not leader" response with a
// LeaderHint is followed immediately (no backoff — the cluster already
// told us where to go). Anything else that isn't a clean success
// (network error, "no leader" mid-election, or "not leader" with no
// hint yet) triggers a short backoff and a switch to the next node in
// addrs, on the theory that a different node might have fresher
// information. Gives up once totalTimeout has elapsed.
func sendWithRetry(addrs []string, timeout, totalTimeout time.Duration, req proto.ClientRequest) (proto.ClientResponse, error) {
	deadline := time.Now().Add(totalTimeout)
	target := addrs[0]
	var lastErr error
	var lastResp proto.ClientResponse

	for attempt := 0; ; attempt++ {
		resp, err := send(target, timeout, req)
		if err == nil {
			if resp.OK {
				return resp, nil
			}
			if resp.Error == "not leader" && resp.LeaderHint != "" {
				target = resp.LeaderHint
				continue // redirect immediately, no backoff
			}
			lastResp, lastErr = resp, nil
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			if lastErr != nil {
				return proto.ClientResponse{}, fmt.Errorf("no leader found after %d attempts: %w", attempt+1, lastErr)
			}
			return lastResp, nil // surface the last error response (e.g. "no leader") to the caller
		}
		time.Sleep(backoff(attempt))
		target = addrs[(attempt+1)%len(addrs)]
	}
}

func backoff(attempt int) time.Duration {
	d := time.Duration(50*(attempt+1)) * time.Millisecond
	if d > 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return d
}

func send(addr string, timeout time.Duration, req proto.ClientRequest) (proto.ClientResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return proto.ClientResponse{}, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	if err := proto.WriteMessage(conn, req); err != nil {
		return proto.ClientResponse{}, fmt.Errorf("send request: %w", err)
	}
	var resp proto.ClientResponse
	if err := proto.ReadMessage(conn, &resp); err != nil {
		return proto.ClientResponse{}, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  quorumctl [-addrs host:port,...] [-timeout d] [-total-timeout d] get <key>
  quorumctl [-addrs host:port,...] [-timeout d] [-total-timeout d] set <key> <value>
  quorumctl [-addrs host:port,...] [-timeout d] [-total-timeout d] delete <key>`)
}
