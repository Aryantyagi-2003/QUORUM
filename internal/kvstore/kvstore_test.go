package kvstore

import "testing"

func TestStore_SetGetDelete(t *testing.T) {
	s := New()

	if _, found := s.Get("k"); found {
		t.Fatal("Get on empty store should not find key")
	}

	s.Apply(Command{Op: OpSet, Key: "k", Value: "v1"})
	if v, found := s.Get("k"); !found || v != "v1" {
		t.Fatalf("Get(k) = %q, %v, want v1, true", v, found)
	}

	s.Apply(Command{Op: OpSet, Key: "k", Value: "v2"})
	if v, _ := s.Get("k"); v != "v2" {
		t.Fatalf("Get(k) after overwrite = %q, want v2", v)
	}

	s.Apply(Command{Op: OpDelete, Key: "k"})
	if _, found := s.Get("k"); found {
		t.Fatal("Get(k) after delete should not find key")
	}
}

func TestStore_DeduplicatesRetriedCommands(t *testing.T) {
	s := New()

	cmd := Command{Op: OpSet, Key: "k", Value: "v1", ClientID: "c1", SeqNum: 1}
	s.Apply(cmd)
	// Simulate the same command being applied twice, as could happen if
	// a client retry caused it to appear in the log a second time.
	s.Apply(cmd)

	s.Apply(Command{Op: OpSet, Key: "k", Value: "v2", ClientID: "c1", SeqNum: 1})
	if v, _ := s.Get("k"); v != "v1" {
		t.Fatalf("Get(k) = %q, want v1 (duplicate SeqNum must be ignored)", v)
	}

	s.Apply(Command{Op: OpSet, Key: "k", Value: "v3", ClientID: "c1", SeqNum: 2})
	if v, _ := s.Get("k"); v != "v3" {
		t.Fatalf("Get(k) = %q, want v3 (new SeqNum must apply)", v)
	}
}

func TestEncodeDecodeCommand_RoundTrip(t *testing.T) {
	want := Command{Op: OpSet, Key: "k", Value: "v", ClientID: "c1", SeqNum: 3}
	b, err := EncodeCommand(want)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	got, err := DecodeCommand(b)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got != want {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}
