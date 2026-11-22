package multipath

import (
	"errors"
	"testing"

	"github.com/bojieli/queqiao/internal/memlimit"
)

func TestDeliveryRetainsBudgetUntilApplicationAccepts(t *testing.T) {
	budget := memlimit.New(8)
	r := NewReassembler(Config{Memory: budget})
	defer r.Close()
	if _, _, err := r.Insert(Segment{Sequence: 4, Payload: []byte("efgh")}); err != nil {
		t.Fatal(err)
	}
	entered, resume, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	calls := 0
	var result string
	go func() {
		_, err := r.InsertTo(Segment{Payload: []byte("abcd")}, func(p []byte) error {
			calls++
			if calls == 1 {
				close(entered)
				<-resume
			}
			result += string(p)
			return nil
		})
		finished <- err
	}()
	<-entered
	used := budget.Snapshot().Used
	admitted := budget.TryAcquire(1)
	if admitted {
		budget.Release(1)
	}
	close(resume)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if used != 8 || admitted {
		t.Fatalf("blocked delivery: used=%d admitted=%v", used, admitted)
	}
	if result != "abcdefgh" || calls != 2 {
		t.Fatalf("delivery = %q in %d chunks", result, calls)
	}
	if got := budget.Snapshot().Used; got != 0 {
		t.Fatalf("after delivery: %d", got)
	}
}

func TestDeliveryFailureReleasesCurrentAndCloseReleasesRemainder(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		budget := memlimit.New(12)
		r := NewReassembler(Config{Memory: budget})
		for _, s := range []Segment{{Sequence: 4, Payload: []byte("efgh")}, {Sequence: 8, Payload: []byte("ijkl")}} {
			if _, _, err := r.Insert(s); err != nil {
				t.Fatal(err)
			}
		}
		sentinel := errors.New("application closed")
		calls := 0
		_, err := r.InsertTo(Segment{Payload: []byte("abcd")}, func([]byte) error {
			calls++
			if calls == failAt {
				return sentinel
			}
			return nil
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("error: %v", err)
		}
		if got, want := budget.Snapshot().Used, int64((3-failAt)*4); got != want {
			t.Fatalf("retained: %d want %d", got, want)
		}
		r.Close()
		r.Close()
		if got := budget.Snapshot().Used; got != 0 {
			t.Fatalf("after close: %d", got)
		}
	}
}
