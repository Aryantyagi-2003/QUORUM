// Package storage implements Quorum's on-disk persistence: the small
// frequently-rewritten HardState (currentTerm, votedFor) and the
// append-only replicated Log, per the Raft paper's §5.4.1 requirement
// that a node never forget these across a crash/restart.
package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// HardState is the minimal state that must be durable before a node
// responds to any RPC that depends on it (Raft paper §5, Figure 2:
// "Persistent state on all servers"). It excludes the log itself,
// which is persisted separately in Log (log.dat) since it has very
// different write characteristics (append-mostly, occasionally large).
type HardState struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"` // "" means no vote cast this term
}

// HardStateStore persists HardState to disk, using write-to-temp-file
// then atomic rename so a crash mid-write can never leave a
// half-written hardstate.json for the next startup to misread.
type HardStateStore struct {
	path string
}

func NewHardStateStore(dir string) *HardStateStore {
	return &HardStateStore{path: filepath.Join(dir, "hardstate.json")}
}

// Load reads the persisted HardState, or returns the zero value if no
// hardstate file exists yet (a brand-new node: term 0, no vote cast).
func (s *HardStateStore) Load() (HardState, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, err
	}
	var hs HardState
	if err := json.Unmarshal(data, &hs); err != nil {
		return HardState{}, err
	}
	return hs, nil
}

// Save durably persists hs before returning, so callers may rely on it
// being crash-safe as soon as Save returns nil.
func (s *HardStateStore) Save(hs HardState) error {
	data, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
