package multipath

import (
	"errors"
	"github.com/bojieli/queqiao/internal/memlimit"
	"testing"
)

func TestReassemblyDeliveredDuplicatesDoNotConsumeWindow(t *testing.T) {
	b := memlimit.New(16)
	r := NewReassembler(Config{MaxBufferedBytes: 16, MaxBufferedFrames: 4, Memory: b})
	defer r.Close()
	for n := uint64(0); n < 100; n++ {
		s := Segment{Sequence: n * 4, Payload: []byte("data")}
		for copy := 0; copy < 2; copy++ {
			out, _, err := r.Insert(s)
			if err != nil {
				t.Fatalf("segment %d copy %d: %v", n, copy, err)
			}
			if copy == 1 && len(out) != 0 {
				t.Fatal("duplicate delivered twice")
			}
		}
		if r.BufferedFrames() != 0 || b.Snapshot().Used != 0 {
			t.Fatal("delivered duplicate retained")
		}
	}
	if _, _, err := r.Insert(Segment{Sequence: 398, Payload: []byte("data")}); err == nil {
		t.Fatal("partial cursor overlap accepted")
	}
}

func TestReassemblyDefaultsPreserveExplicitBudget(t *testing.T) {
	for _, cfg := range []Config{{MaxBufferedFrames: 4, Memory: memlimit.New(2)}, {MaxBufferedBytes: 2, Memory: memlimit.New(8)}} {
		r := NewReassembler(cfg)
		_, _, err := r.Insert(Segment{Sequence: 4, Payload: []byte("data")})
		r.Close()
		if !errors.Is(err, ErrMemoryBudget) && !errors.Is(err, ErrWindowExceeded) {
			t.Fatalf("explicit limit lost: %v", err)
		}
	}
}

func TestReassemblyFinalOffsetCannotDiscardAcceptedData(t *testing.T) {
	cases := []struct {
		name    string
		before  []Segment
		invalid Segment
	}{
		{"conflicting-fin", []Segment{{Sequence: 8, Final: true}}, Segment{Sequence: 4, Final: true}},
		{"fin-before-buffer", []Segment{{Sequence: 8, Payload: []byte("data")}}, Segment{Sequence: 4, Final: true}},
		{"data-after-fin", []Segment{{Sequence: 4, Final: true}}, Segment{Sequence: 5, Payload: []byte("x")}},
		{"fin-before-cursor", []Segment{{Payload: []byte("12345678")}}, Segment{Sequence: 4, Final: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := memlimit.New(64)
			r := NewReassembler(Config{Memory: b})
			defer r.Close()
			for _, s := range tc.before {
				if _, _, err := r.Insert(s); err != nil {
					t.Fatal(err)
				}
			}
			next, frames, size, closed, used := r.NextSequence(), r.BufferedFrames(), r.BufferedBytes(), r.Closed(), b.Snapshot().Used
			if _, _, err := r.Insert(tc.invalid); err == nil {
				t.Fatal("invalid final boundary accepted")
			}
			if next != r.NextSequence() || frames != r.BufferedFrames() || size != r.BufferedBytes() || closed != r.Closed() || used != b.Snapshot().Used {
				t.Fatal("rejected frame mutated state")
			}
		})
	}
}
