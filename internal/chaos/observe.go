package chaos

import (
	"net"
	"time"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

// CurrentLeader queries every node directly (bypassing client.Client's
// redirect-following, which would just find AN answer, not identify
// WHICH node gave it) and returns the ID of whichever one currently
// believes itself the leader. Returns ok=false if no node currently
// claims leadership (e.g. mid-election) or none are reachable.
func CurrentLeader(clientAddrs map[string]string, timeout time.Duration) (leaderID string, ok bool) {
	for id, addr := range clientAddrs {
		resp, err := RawRequest(addr, proto.ClientRequest{RPC: "Get", Key: "__chaos_probe__"}, timeout)
		if err == nil && resp.OK {
			return id, true
		}
	}
	return "", false
}

// RawRequest sends req directly to addr and returns its raw response,
// with no redirect-following. Deliberately distinct from
// client.Client: a probe that needs to know whether THIS SPECIFIC node
// will accept a request (e.g. "does an isolated minority node still
// accept a write") would get a misleading answer from a redirecting
// client, which will happily follow a "not leader" response's
// LeaderHint to a DIFFERENT, still-reachable node and report success
// from there -- correct for a real user, wrong for a test asking about
// one particular node's behavior.
func RawRequest(addr string, req proto.ClientRequest, timeout time.Duration) (proto.ClientResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return proto.ClientResponse{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	if err := proto.WriteMessage(conn, req); err != nil {
		return proto.ClientResponse{}, err
	}
	var resp proto.ClientResponse
	if err := proto.ReadMessage(conn, &resp); err != nil {
		return proto.ClientResponse{}, err
	}
	return resp, nil
}
