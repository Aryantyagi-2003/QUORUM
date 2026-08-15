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
		resp, err := rawGet(addr, "__chaos_probe__", timeout)
		if err == nil && resp.OK {
			return id, true
		}
	}
	return "", false
}

func rawGet(addr, key string, timeout time.Duration) (proto.ClientResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return proto.ClientResponse{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	if err := proto.WriteMessage(conn, proto.ClientRequest{RPC: "Get", Key: key}); err != nil {
		return proto.ClientResponse{}, err
	}
	var resp proto.ClientResponse
	if err := proto.ReadMessage(conn, &resp); err != nil {
		return proto.ClientResponse{}, err
	}
	return resp, nil
}
