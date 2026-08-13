package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Aryantyagi-2003/Quorum/internal/proto"
)

// Log is Quorum's on-disk replicated log: an append-only binary file
// (log.dat) plus an in-memory mirror for fast reads. A hand-rolled
// format rather than an embedded KV engine (e.g. BoltDB) because the
// access pattern — append to the tail, occasionally truncate the tail
// on a conflict (Raft paper §5.3), never touch the middle — is exactly
// what a flat append-only file is best at, and building it directly
// demonstrates the durability mechanics this project is about.
//
// On-disk record layout, one after another for as many entries as
// exist:
//
//	[4B recordLen][8B term][8B index][4B commandLen][commandLen bytes command][4B CRC32]
//
// recordLen covers everything from term through the command (not
// itself, not the trailing CRC). The CRC32 covers term+index+commandLen+
// command and is used at replay time to detect a torn write left by a
// crash mid-append: a record whose CRC doesn't match is treated as an
// incomplete final write and the file is truncated to just before it,
// rather than treated as corruption of the whole log.
type Log struct {
	mu      sync.RWMutex
	path    string
	f       *os.File
	entries []proto.LogEntry // index 0 unused; entries[i] has Index == i
	offsets []int64          // offsets[i] = byte offset in file where entries[i]'s record starts
}

const recordFixedOverhead = 4 + 8 + 8 + 4 + 4 // recordLen + term + index + commandLen + crc32

// OpenLog opens (creating if necessary) the log.dat file in dir and
// replays it into memory. Any trailing torn write (partial record left
// by a crash mid-append) is truncated from the file, since it was
// never fsync'd as complete and so was never acknowledged to anyone.
func OpenLog(dir string) (*Log, error) {
	path := filepath.Join(dir, "log.dat")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storage: open log: %w", err)
	}

	l := &Log{
		path:    path,
		f:       f,
		entries: []proto.LogEntry{{}}, // sentinel at index 0
		offsets: []int64{0},
	}
	if err := l.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// replay reads every record from the file into memory, stopping (and
// truncating the file) at the first incomplete or corrupt record.
func (l *Log) replay() error {
	r := bufio.NewReader(l.f)
	var offset int64
	for {
		startOffset := offset
		entry, n, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Torn or corrupt record: this is the crash-recovery case —
			// truncate the file back to the last known-good record so
			// the log doesn't carry a partially-written entry forward.
			if truncErr := l.f.Truncate(startOffset); truncErr != nil {
				return fmt.Errorf("storage: truncate torn write: %w", truncErr)
			}
			break
		}
		l.entries = append(l.entries, entry)
		l.offsets = append(l.offsets, startOffset)
		offset += int64(n)
	}
	if _, err := l.f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("storage: seek after replay: %w", err)
	}
	return nil
}

// readRecord reads one record from r, returning the decoded entry and
// the total number of bytes the record occupied on disk (recordLen
// prefix + body + trailing CRC).
func readRecord(r io.Reader) (proto.LogEntry, int, error) {
	var recLenBuf [4]byte
	if _, err := io.ReadFull(r, recLenBuf[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return proto.LogEntry{}, 0, fmt.Errorf("torn record length prefix")
		}
		return proto.LogEntry{}, 0, err // clean io.EOF at a record boundary
	}
	recLen := binary.BigEndian.Uint32(recLenBuf[:])

	body := make([]byte, recLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return proto.LogEntry{}, 0, fmt.Errorf("torn record body: %w", err)
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return proto.LogEntry{}, 0, fmt.Errorf("torn record crc: %w", err)
	}
	wantCRC := binary.BigEndian.Uint32(crcBuf[:])
	if gotCRC := crc32.ChecksumIEEE(body); gotCRC != wantCRC {
		return proto.LogEntry{}, 0, fmt.Errorf("crc mismatch: got %x want %x", gotCRC, wantCRC)
	}

	if len(body) < 20 {
		return proto.LogEntry{}, 0, fmt.Errorf("record body too short: %d bytes", len(body))
	}
	term := binary.BigEndian.Uint64(body[0:8])
	index := binary.BigEndian.Uint64(body[8:16])
	cmdLen := binary.BigEndian.Uint32(body[16:20])
	if uint32(len(body)-20) != cmdLen {
		return proto.LogEntry{}, 0, fmt.Errorf("command length mismatch: header says %d, got %d", cmdLen, len(body)-20)
	}
	command := body[20:]

	total := 4 + int(recLen) + 4
	return proto.LogEntry{Term: term, Index: index, Command: command}, total, nil
}

func encodeRecord(e proto.LogEntry) []byte {
	body := make([]byte, 20+len(e.Command))
	binary.BigEndian.PutUint64(body[0:8], e.Term)
	binary.BigEndian.PutUint64(body[8:16], e.Index)
	binary.BigEndian.PutUint32(body[16:20], uint32(len(e.Command)))
	copy(body[20:], e.Command)

	out := make([]byte, 4+len(body)+4)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(body)))
	copy(out[4:4+len(body)], body)
	binary.BigEndian.PutUint32(out[4+len(body):], crc32.ChecksumIEEE(body))
	return out
}

// Append writes entries to the end of the log and fsyncs before
// returning, so a caller may reply to an AppendEntries RPC as soon as
// Append returns nil and know the entries will survive a crash (Raft
// paper §5: persistent state must be updated on stable storage before
// responding to RPCs).
//
// Callers are responsible for truncating conflicting entries first
// (via TruncateFrom) per the log matching property; Append itself is a
// pure append and will produce a gap or duplicate indices if misused.
func (l *Log) Append(entries []proto.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	offset, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("storage: seek to end: %w", err)
	}

	var buf []byte
	newOffsets := make([]int64, 0, len(entries))
	for _, e := range entries {
		newOffsets = append(newOffsets, offset+int64(len(buf)))
		buf = append(buf, encodeRecord(e)...)
	}
	if _, err := l.f.Write(buf); err != nil {
		return fmt.Errorf("storage: write entries: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync: %w", err)
	}

	l.entries = append(l.entries, entries...)
	l.offsets = append(l.offsets, newOffsets...)
	return nil
}

// TruncateFrom discards all entries with index >= fromIndex, both in
// memory and on disk, and fsyncs the truncation. Used when a follower
// finds its log conflicts with the leader's at fromIndex (Raft paper
// §5.3: "If an existing entry conflicts with a new one ... delete the
// existing entry and all that follow it").
func (l *Log) TruncateFrom(fromIndex uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if fromIndex >= uint64(len(l.entries)) {
		return nil // nothing to truncate
	}
	if fromIndex == 0 {
		return fmt.Errorf("storage: cannot truncate index 0 (sentinel)")
	}

	offset := l.offsets[fromIndex]
	if err := l.f.Truncate(offset); err != nil {
		return fmt.Errorf("storage: truncate: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync after truncate: %w", err)
	}
	l.entries = l.entries[:fromIndex]
	l.offsets = l.offsets[:fromIndex]
	return nil
}

// LastIndex returns the index of the last entry in the log, or 0 if
// the log is empty.
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.entries) - 1)
}

// LastTerm returns the term of the last entry in the log, or 0 if the
// log is empty.
func (l *Log) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at index, or (0, false) if
// index is out of range (including the index-0 sentinel).
func (l *Log) TermAt(index uint64) (uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 || index >= uint64(len(l.entries)) {
		return 0, false
	}
	return l.entries[index].Term, true
}

// Get returns the entry at index, or (LogEntry{}, false) if out of
// range.
func (l *Log) Get(index uint64) (proto.LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 || index >= uint64(len(l.entries)) {
		return proto.LogEntry{}, false
	}
	return l.entries[index], true
}

// Slice returns a copy of entries [from, end of log], for building an
// AppendEntries RPC payload.
func (l *Log) Slice(from uint64) []proto.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if from >= uint64(len(l.entries)) {
		return nil
	}
	if from == 0 {
		from = 1
	}
	out := make([]proto.LogEntry, len(l.entries)-int(from))
	copy(out, l.entries[from:])
	return out
}

func (l *Log) Close() error {
	return l.f.Close()
}
