package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Each sample lands in the first bucket whose bound it does not exceed, a
// negative one at zero and one past the last bound in +Inf; the export is a
// Prometheus histogram with cumulative buckets, a sum and a count.
func TestCreditDelayHistogram(t *testing.T) {
	r := New()
	for _, d := range []time.Duration{-time.Millisecond, time.Millisecond, 12 * time.Millisecond, 100 * time.Millisecond, time.Minute} {
		r.CreditDelay(d)
	}
	var nilRegistry *Registry
	nilRegistry.CreditDelay(time.Second) // metrics off: a no-op
	s := r.Snapshot().CreditDelay
	if s.Count() != 5 {
		t.Fatalf("count %d, want 5", s.Count())
	}
	want := map[int]uint64{0: 2, 4: 1, 10: 1, CreditDelayBuckets - 1: 1}
	for i, v := range s.Buckets {
		if v != want[i] {
			t.Errorf("bucket %d holds %d, want %d", i, v, want[i])
		}
	}
	if s.Sum != time.Millisecond+12*time.Millisecond+100*time.Millisecond+time.Minute {
		t.Errorf("sum %v", s.Sum)
	}
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	page := recorder.Body.String()
	for _, line := range []string{
		`queqiao_credit_delay_seconds_bucket{le="0.001"} 2`,
		`queqiao_credit_delay_seconds_bucket{le="0.015"} 3`,
		`queqiao_credit_delay_seconds_bucket{le="0.1"} 4`,
		`queqiao_credit_delay_seconds_bucket{le="30"} 4`,
		`queqiao_credit_delay_seconds_bucket{le="+Inf"} 5`,
		`queqiao_credit_delay_seconds_count 5`,
		`queqiao_credit_delay_seconds_sum 60.113000000`,
	} {
		if !strings.Contains(page, line+"\n") {
			t.Errorf("/metrics lacks %q", line)
		}
	}
}
