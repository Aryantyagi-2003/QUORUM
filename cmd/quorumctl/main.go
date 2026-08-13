// Command quorumctl is a CLI client for a Quorum cluster.
//
// Usage:
//
//	quorumctl -addr 127.0.0.1:7000 get <key>
//	quorumctl -addr 127.0.0.1:7000 set <key> <value>
//	quorumctl -addr 127.0.0.1:7000 delete <key>
//
// Phase 1: talks to a single node directly at -addr. Once replication
// and leader redirection exist, this will grow leader-tracking (retry
// against the LeaderHint on a "not leader" response) instead of
// requiring the caller to already know the leader's address.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "node address")
	timeout := flag.Duration("timeout", 3*time.Second, "request timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
		os.Exit(2)
	}

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

	resp, err := send(*addr, *timeout, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "quorumctl: %v\n", err)
		os.Exit(1)
	}
	if !resp.OK {
		fmt.Fprintf(os.Stderr, "quorumctl: error: %s\n", resp.Error)
		if resp.LeaderHint != "" {
			fmt.Fprintf(os.Stderr, "quorumctl: leader hint: %s\n", resp.LeaderHint)
		}
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
  quorumctl [-addr host:port] [-timeout d] get <key>
  quorumctl [-addr host:port] [-timeout d] set <key> <value>
  quorumctl [-addr host:port] [-timeout d] delete <key>`)
}
