// Package client implements a redirect-and-retry client for a Quorum
// cluster. Factored out of cmd/quorumctl so the chaos harness's
// workload goroutines (Phase 5) exercise the exact same client
// behavior a real user gets, rather than a parallel reimplementation
// that could quietly drift from it.
package client

import (
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

// Client is a redirect-and-retry client for a Quorum cluster.
//
// Not safe for concurrent use from multiple goroutines: each
// concurrent writer should construct its own Client (and therefore its
// own ClientID), matching the per-client SeqNum dedup model the server
// already implements (internal/kvstore's ClientID/SeqNum-keyed apply
// dedup).
type Client struct {
	addrs        []string
	timeout      time.Duration
	totalTimeout time.Duration
	clientID     string
	seqNum       uint64
	knownLeader  string // last address that actually worked; tried first on the next call
}

func New(addrs []string, timeout, totalTimeout time.Duration) *Client {
	return &Client{
		addrs:        addrs,
		timeout:      timeout,
		totalTimeout: totalTimeout,
		clientID:     fmt.Sprintf("client-%d", rand.Int63()),
	}
}

// ResponseError wraps a clean (non-network) error response from the
// server -- e.g. "not leader" or "no leader" -- so callers can tell
// "the cluster told us it couldn't do this right now" apart from a
// network-level failure, if they need to.
type ResponseError struct{ Err string }

func (e *ResponseError) Error() string { return e.Err }

func (c *Client) Get(key string) (value string, found bool, err error) {
	resp, err := c.do(proto.ClientRequest{RPC: "Get", Key: key, ClientID: c.clientID})
	if err != nil {
		return "", false, err
	}
	if !resp.OK {
		return "", false, &ResponseError{Err: resp.Error}
	}
	return resp.Value, resp.Found, nil
}

func (c *Client) Set(key, value string) error {
	c.seqNum++
	resp, err := c.do(proto.ClientRequest{RPC: "Set", Key: key, Value: value, ClientID: c.clientID, SeqNum: c.seqNum})
	if err != nil {
		return err
	}
	if !resp.OK {
		return &ResponseError{Err: resp.Error}
	}
	return nil
}

func (c *Client) Delete(key string) error {
	c.seqNum++
	resp, err := c.do(proto.ClientRequest{RPC: "Delete", Key: key, ClientID: c.clientID, SeqNum: c.seqNum})
	if err != nil {
		return err
	}
	if !resp.OK {
		return &ResponseError{Err: resp.Error}
	}
	return nil
}

// do tries the last known-good leader first (falling back to addrs[0]
// on a fresh Client), follows a "not leader" LeaderHint immediately
// (no backoff -- the cluster already told us where to go), and backs
// off + rotates to a different configured node on anything else
// (network error, "no leader" mid-election, or "not leader" with no
// hint yet). Gives up once totalTimeout has elapsed, surfacing the
// last clean error response (e.g. "no leader") if there was one, or
// the last network error otherwise.
func (c *Client) do(req proto.ClientRequest) (proto.ClientResponse, error) {
	deadline := time.Now().Add(c.totalTimeout)
	target := c.knownLeader
	if target == "" {
		target = c.addrs[0]
	}
	var lastErr error
	var lastResp proto.ClientResponse

	for attempt := 0; ; attempt++ {
		resp, err := c.send(target, req)
		if err == nil {
			if resp.OK {
				c.knownLeader = target
				return resp, nil
			}
			if resp.Error == "not leader" && resp.LeaderHint != "" {
				target = resp.LeaderHint
				continue
			}
			lastResp, lastErr = resp, nil
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			if lastErr != nil {
				return proto.ClientResponse{}, fmt.Errorf("no leader found after %d attempts: %w", attempt+1, lastErr)
			}
			return lastResp, nil
		}
		time.Sleep(backoff(attempt))
		target = c.addrs[(attempt+1)%len(c.addrs)]
	}
}

func backoff(attempt int) time.Duration {
	d := time.Duration(50*(attempt+1)) * time.Millisecond
	if d > 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return d
}

func (c *Client) send(addr string, req proto.ClientRequest) (proto.ClientResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return proto.ClientResponse{}, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(c.timeout))

	if err := proto.WriteMessage(conn, req); err != nil {
		return proto.ClientResponse{}, fmt.Errorf("send request: %w", err)
	}
	var resp proto.ClientResponse
	if err := proto.ReadMessage(conn, &resp); err != nil {
		return proto.ClientResponse{}, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}
