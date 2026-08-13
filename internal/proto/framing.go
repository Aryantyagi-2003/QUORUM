package proto

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxMessageSize bounds a single framed message to guard against a
// corrupt or malicious length prefix causing an unbounded allocation.
const MaxMessageSize = 16 * 1024 * 1024 // 16 MiB

// WriteMessage frames v as [4-byte big-endian length][JSON body] and
// writes it to w.
func WriteMessage(w io.Writer, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("proto: marshal: %w", err)
	}
	if len(body) > MaxMessageSize {
		return fmt.Errorf("proto: message too large: %d bytes", len(body))
	}
	var lenPrefix [4]byte
	binary.BigEndian.PutUint32(lenPrefix[:], uint32(len(body)))
	if _, err := w.Write(lenPrefix[:]); err != nil {
		return fmt.Errorf("proto: write length prefix: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("proto: write body: %w", err)
	}
	return nil
}

// ReadMessage reads one framed message from r and unmarshals its JSON
// body into v.
func ReadMessage(r io.Reader, v any) error {
	body, err := ReadFrame(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("proto: unmarshal: %w", err)
	}
	return nil
}

// ReadFrame reads one length-prefixed frame from r and returns its raw
// JSON body, without decoding it. Used by dispatchers that must peek
// the "rpc" type tag (via Envelope) before choosing which concrete
// struct to unmarshal into.
func ReadFrame(r io.Reader) ([]byte, error) {
	var lenPrefix [4]byte
	if _, err := io.ReadFull(r, lenPrefix[:]); err != nil {
		return nil, err // may be io.EOF; callers rely on that to detect conn close
	}
	n := binary.BigEndian.Uint32(lenPrefix[:])
	if n > MaxMessageSize {
		return nil, fmt.Errorf("proto: message too large: %d bytes", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("proto: read body: %w", err)
	}
	return body, nil
}

// Envelope is used to peek at the "rpc" type tag before decoding the
// full body into the concrete args struct.
type Envelope struct {
	RPC string `json:"rpc"`
}
