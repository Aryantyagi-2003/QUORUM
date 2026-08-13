package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
)

func TestWriteReadMessage_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := RequestVoteArgs{
		RPC: "RequestVote", Term: 7, CandidateID: "node-1",
		LastLogIndex: 42, LastLogTerm: 6,
	}
	if err := WriteMessage(&buf, want); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	var got RequestVoteArgs
	if err := ReadMessage(&buf, &got); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if got != want {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

func TestWriteReadMessage_MultipleFramesOnSameStream(t *testing.T) {
	var buf bytes.Buffer
	msgs := []AppendEntriesArgs{
		{RPC: "AppendEntries", Term: 1, LeaderID: "a"},
		{RPC: "AppendEntries", Term: 2, LeaderID: "b"},
		{RPC: "AppendEntries", Term: 3, LeaderID: "c"},
	}
	for _, m := range msgs {
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
	}
	for i, want := range msgs {
		var got AppendEntriesArgs
		if err := ReadMessage(&buf, &got); err != nil {
			t.Fatalf("ReadMessage frame %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("frame %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestReadFrame_EnvelopeDispatch(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, AppendEntriesArgs{RPC: "AppendEntries", Term: 9}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	body, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.RPC != "AppendEntries" {
		t.Errorf("env.RPC = %q, want %q", env.RPC, "AppendEntries")
	}
}

func TestReadMessage_EOFOnEmptyStream(t *testing.T) {
	var buf bytes.Buffer
	var got RequestVoteArgs
	err := ReadMessage(&buf, &got)
	if err == nil {
		t.Fatal("expected error reading from empty stream, got nil")
	}
}

func TestReadFrame_RejectsOversizedMessage(t *testing.T) {
	var buf bytes.Buffer
	var lenPrefix [4]byte
	// Claim a body larger than MaxMessageSize without actually writing
	// that much data — must be rejected before attempting to read it.
	binary.BigEndian.PutUint32(lenPrefix[:], MaxMessageSize+1)
	buf.Write(lenPrefix[:])

	_, err := ReadFrame(&buf)
	if err == nil {
		t.Fatal("expected error for oversized message, got nil")
	}
}
