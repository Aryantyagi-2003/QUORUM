package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

func mustOpenLog(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestLog_AppendAndGet(t *testing.T) {
	dir := t.TempDir()
	l := mustOpenLog(t, dir)

	entries := []proto.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}
	if err := l.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if got := l.LastIndex(); got != 3 {
		t.Errorf("LastIndex() = %d, want 3", got)
	}
	if got := l.LastTerm(); got != 2 {
		t.Errorf("LastTerm() = %d, want 2", got)
	}

	for _, want := range entries {
		got, ok := l.Get(want.Index)
		if !ok {
			t.Fatalf("Get(%d): not found", want.Index)
		}
		if got.Term != want.Term || string(got.Command) != string(want.Command) {
			t.Errorf("Get(%d) = %+v, want %+v", want.Index, got, want)
		}
	}

	if _, ok := l.Get(0); ok {
		t.Error("Get(0) should be not-found (sentinel)")
	}
	if _, ok := l.Get(99); ok {
		t.Error("Get(99) should be not-found (out of range)")
	}
}

func TestLog_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	l1 := mustOpenLog(t, dir)
	entries := []proto.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}
	if err := l1.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2 := mustOpenLog(t, dir)
	if got := l2.LastIndex(); got != 2 {
		t.Fatalf("after reopen, LastIndex() = %d, want 2", got)
	}
	e, ok := l2.Get(2)
	if !ok || string(e.Command) != "b" {
		t.Fatalf("after reopen, Get(2) = %+v, %v, want command 'b'", e, ok)
	}
}

func TestLog_TruncateFrom(t *testing.T) {
	dir := t.TempDir()
	l := mustOpenLog(t, dir)

	if err := l.Append([]proto.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := l.TruncateFrom(2); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	if got := l.LastIndex(); got != 1 {
		t.Fatalf("LastIndex() after truncate = %d, want 1", got)
	}
	if _, ok := l.Get(2); ok {
		t.Error("Get(2) should be gone after truncate")
	}

	// Overwrite with conflicting entries at index 2 (simulating a
	// leader replacing a follower's stale suffix), and verify the
	// on-disk file reflects the overwrite after reopen too.
	if err := l.Append([]proto.LogEntry{
		{Term: 2, Index: 2, Command: []byte("b2")},
	}); err != nil {
		t.Fatalf("Append after truncate: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2 := mustOpenLog(t, dir)
	if got := l2.LastIndex(); got != 2 {
		t.Fatalf("after reopen, LastIndex() = %d, want 2", got)
	}
	e, ok := l2.Get(2)
	if !ok || e.Term != 2 || string(e.Command) != "b2" {
		t.Fatalf("after reopen, Get(2) = %+v, %v, want term 2 command 'b2'", e, ok)
	}
}

func TestLog_ReplayTruncatesTornWrite(t *testing.T) {
	dir := t.TempDir()
	l := mustOpenLog(t, dir)

	if err := l.Append([]proto.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	goodSize := fileSize(t, dir)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-append: append a second record's bytes but
	// chop off the tail (as if the process died mid-write, before
	// fsync completed).
	path := filepath.Join(dir, "log.dat")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	partial := encodeRecord(proto.LogEntry{Term: 1, Index: 2, Command: []byte("b")})
	// Only write the first half of the record to simulate a torn write.
	if _, err := f.Write(partial[:len(partial)/2]); err != nil {
		t.Fatalf("write partial record: %v", err)
	}
	f.Close()

	l2 := mustOpenLog(t, dir)
	if got := l2.LastIndex(); got != 1 {
		t.Fatalf("after replay of torn write, LastIndex() = %d, want 1 (torn record must be dropped)", got)
	}
	l2.Close()

	if got := fileSize(t, dir); got != goodSize {
		t.Errorf("file size after replay+truncate = %d, want %d (torn bytes should be truncated on disk)", got, goodSize)
	}
}

func fileSize(t *testing.T, dir string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, "log.dat"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return fi.Size()
}

func TestLog_ReplayDetectsCRCCorruption(t *testing.T) {
	dir := t.TempDir()
	l := mustOpenLog(t, dir)
	if err := l.Append([]proto.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte inside the second record's command to corrupt its CRC.
	path := filepath.Join(dir, "log.dat")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	firstRecLen := binary.BigEndian.Uint32(data[0:4])
	secondRecordCommandOffset := 4 + int(firstRecLen) + 4 + 4 + 20 // skip rec1 fully, then rec2's header
	data[secondRecordCommandOffset] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write corrupted: %v", err)
	}

	l2 := mustOpenLog(t, dir)
	defer l2.Close()
	if got := l2.LastIndex(); got != 1 {
		t.Fatalf("after replay of CRC-corrupted record, LastIndex() = %d, want 1", got)
	}
}

func TestHardStateStore_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	s := NewHardStateStore(dir)

	initial, err := s.Load()
	if err != nil {
		t.Fatalf("Load (no file yet): %v", err)
	}
	if initial != (HardState{}) {
		t.Errorf("Load with no file = %+v, want zero value", initial)
	}

	want := HardState{CurrentTerm: 5, VotedFor: "node-2"}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// A fresh store instance must see the persisted value (proves it's
	// actually on disk, not just cached in the struct).
	s2 := NewHardStateStore(dir)
	got, err := s2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != want {
		t.Errorf("Load() = %+v, want %+v", got, want)
	}
}
