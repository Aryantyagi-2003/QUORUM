package chaos

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// PeerSpec is one other node as this node will reach it -- RaftAddr
// here is a proxy address (see ProxyNetwork), not the peer's real
// Raft listen address, so the harness can drop/heal the link later.
type PeerSpec struct {
	ID, RaftAddr, ClientAddr string
}

// NodeConfig is everything needed to launch (and, on restart,
// relaunch identically) one quorumd process.
type NodeConfig struct {
	ID                       string
	DataDir                  string
	RaftAddr, ClientAddr     string // this node's own real listen addresses
	Peers                    []PeerSpec
	ElectionMin, ElectionMax time.Duration
	Heartbeat, RPCTimeout    time.Duration
	BinaryPath               string
}

// Node manages one real quorumd process: starting it, killing it with
// a real SIGKILL, and restarting it from the same -id/-data-dir so a
// restart genuinely exercises the durability guarantees proven in
// Phases 1-4, not a fresh node standing in for a recovered one.
type Node struct {
	cfg NodeConfig

	mu      sync.Mutex
	cmd     *exec.Cmd
	logFile *os.File
}

func NewNode(cfg NodeConfig) *Node {
	return &Node{cfg: cfg}
}

func (n *Node) ID() string         { return n.cfg.ID }
func (n *Node) ClientAddr() string { return n.cfg.ClientAddr }

// Start launches the process. Safe to call again after Kill (that's a
// restart, sharing the same DataDir).
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := os.MkdirAll(n.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("chaos: mkdir %s: %w", n.cfg.DataDir, err)
	}
	logFile, err := os.OpenFile(filepath.Join(n.cfg.DataDir, "quorumd.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("chaos: open log file: %w", err)
	}

	args := []string{
		"-id", n.cfg.ID,
		"-data-dir", n.cfg.DataDir,
		"-raft-addr", n.cfg.RaftAddr,
		"-client-addr", n.cfg.ClientAddr,
		"-election-timeout-min", n.cfg.ElectionMin.String(),
		"-election-timeout-max", n.cfg.ElectionMax.String(),
		"-heartbeat-interval", n.cfg.Heartbeat.String(),
		"-rpc-timeout", n.cfg.RPCTimeout.String(),
	}
	for _, p := range n.cfg.Peers {
		args = append(args, "-peer", fmt.Sprintf("%s=%s=%s", p.ID, p.RaftAddr, p.ClientAddr))
	}

	cmd := exec.Command(n.cfg.BinaryPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("chaos: start %s: %w", n.cfg.ID, err)
	}

	n.cmd = cmd
	n.logFile = logFile
	return nil
}

// Kill sends a real SIGKILL (kill -9 semantics, not a graceful
// shutdown) and reaps the process. Safe to call on an already-dead or
// never-started node.
func (n *Node) Kill() error {
	n.mu.Lock()
	cmd, logFile := n.cmd, n.logFile
	n.cmd, n.logFile = nil, nil
	n.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}
	err := cmd.Process.Kill()
	cmd.Wait() // reap; ignore the resulting "signal: killed" error, that's expected
	if logFile != nil {
		logFile.Close()
	}
	return err
}

// Restart kills (if running) and starts fresh, reusing the same
// DataDir -- this is what proves a "restarted" node recovers its
// state from disk rather than starting clean.
func (n *Node) Restart() error {
	if err := n.Kill(); err != nil {
		return err
	}
	return n.Start()
}

// Alive reports whether the process is still running, by signaling it
// with signal 0 (a standard, side-effect-free liveness probe).
func (n *Node) Alive() bool {
	n.mu.Lock()
	cmd := n.cmd
	n.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return false
	}
	return cmd.Process.Signal(syscall.Signal(0)) == nil
}
