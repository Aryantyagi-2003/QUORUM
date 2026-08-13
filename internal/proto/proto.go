// Package proto defines Quorum's wire protocol: message shapes for the
// inter-node Raft RPCs (RequestVote, AppendEntries) and the client-facing
// KV protocol, plus the length-prefixed JSON framing used to send them
// over TCP.
package proto

// LogEntry is a single entry in a node's replicated log.
type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"` // opaque, encoded/decoded by the KV layer
}

// RequestVoteArgs is sent by a candidate to gather votes.
// Raft paper §5.2: candidates request votes with their last log's
// index/term so voters can enforce the "at least as up-to-date" check.
type RequestVoteArgs struct {
	RPC          string `json:"rpc"` // "RequestVote"
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

// RequestVoteReply is a voter's response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// AppendEntriesArgs is sent by a leader to replicate log entries and,
// when Entries is empty, to serve as a heartbeat (Raft paper §5.3).
type AppendEntriesArgs struct {
	RPC          string     `json:"rpc"` // "AppendEntries"
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries,omitempty"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

// AppendEntriesReply is a follower's response to an AppendEntries RPC.
// ConflictIndex/ConflictTerm implement the paper's §5.3 footnote
// optimization: on rejection, the follower tells the leader enough to
// back up nextIndex by more than one entry per round trip.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflictIndex,omitempty"`
	ConflictTerm  uint64 `json:"conflictTerm,omitempty"`
}

// Client-facing KV protocol.

// ClientRequest is a KV operation sent by a client to a node.
// ClientID and SeqNum implement the paper's §8 duplicate-detection
// scheme: a client retrying an unacknowledged write after a leader
// change must not have that write applied twice.
type ClientRequest struct {
	RPC      string `json:"rpc"` // "Get" | "Set" | "Delete"
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID string `json:"clientId"`
	SeqNum   uint64 `json:"seqNum"`
}

// ClientResponse is a node's response to a ClientRequest.
type ClientResponse struct {
	OK         bool   `json:"ok"`
	Value      string `json:"value,omitempty"`
	Found      bool   `json:"found,omitempty"` // for Get: whether the key existed
	Error      string `json:"error,omitempty"` // "not leader" | "no leader" | "timeout" | ...
	LeaderHint string `json:"leaderHint,omitempty"`
}
