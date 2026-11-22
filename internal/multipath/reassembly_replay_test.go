package multipath

import "testing"

func TestReplayAtReceiveCursorDoesNotConsumeWindow(t *testing.T) {
	r := NewReassembler(Config{MaxBufferedBytes: 8, MaxBufferedFrames: 1})
	defer r.Close()
	if _, _, err := r.Insert(Segment{Sequence: 0, Payload: []byte("response")}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		out, _, err := r.Insert(Segment{Sequence: uint64(i % 8), Payload: []byte("response")[i%8:]})
		if err != nil || len(out) != 0 {
			t.Fatalf("duplicate %d: out=%q err=%v", i, out, err)
		}
		if r.BufferedBytes() != 0 || r.BufferedFrames() != 0 {
			t.Fatalf("delivered replay retained: bytes=%d frames=%d", r.BufferedBytes(), r.BufferedFrames())
		}
	}
	if out, _, err := r.Insert(Segment{Sequence: 8, Payload: []byte("next")}); err != nil || string(out) != "next" {
		t.Fatalf("next: %q, %v", out, err)
	}
}

func TestReplayCrossingReceiveCursorIsRejected(t *testing.T) {
	r := NewReassembler(DefaultConfig())
	defer r.Close()
	if _, _, err := r.Insert(Segment{Sequence: 0, Payload: []byte("done")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Insert(Segment{Sequence: 3, Payload: []byte("excess")}); err == nil {
		t.Fatal("accepted overlapping replay across delivered cursor")
	}
}
