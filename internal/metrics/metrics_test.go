package metrics

import (
	"math"
	"strings"
	"sync"
	"testing"
)

func TestCounterSeriesAreIndependent(t *testing.T) {
	r := NewRegistry()
	r.DefineCounter("payments_total", "Payments by outcome.")
	r.Inc("payments_total", Labels{"outcome": "created"})
	r.Inc("payments_total", Labels{"outcome": "created"})
	r.Inc("payments_total", Labels{"outcome": "replayed"})
	r.Add("payments_total", Labels{"outcome": "rejected"}, 5)

	if got := r.CounterValue("payments_total", Labels{"outcome": "created"}); got != 2 {
		t.Errorf("created = %d, want 2", got)
	}
	if got := r.CounterValue("payments_total", Labels{"outcome": "replayed"}); got != 1 {
		t.Errorf("replayed = %d, want 1", got)
	}
	if got := r.CounterValue("payments_total", Labels{"outcome": "rejected"}); got != 5 {
		t.Errorf("rejected = %d, want 5", got)
	}
	if got := r.CounterValue("payments_total", Labels{"outcome": "never"}); got != 0 {
		t.Errorf("absent series = %d, want 0", got)
	}
}

func TestLabelOrderDoesNotCreateDuplicateSeries(t *testing.T) {
	a := seriesKey("m", Labels{"b": "2", "a": "1"})
	b := seriesKey("m", Labels{"a": "1", "b": "2"})
	if a != b {
		t.Fatalf("label order changed the series key: %q vs %q", a, b)
	}
	if want := `m{a="1",b="2"}`; a != want {
		t.Errorf("seriesKey = %q, want %q", a, want)
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	got := seriesKey("m", Labels{"path": `a"b\c` + "\n"})
	if !strings.Contains(got, `a\"b\\c\n`) {
		t.Errorf("seriesKey = %q, want escaped quotes, backslashes and newlines", got)
	}
}

func TestHistogramQuantiles(t *testing.T) {
	r := NewRegistry()
	r.DefineHistogram("latency_seconds", "Latency.", []float64{0.01, 0.1, 1})
	// 90 fast observations and 10 slow ones: p95 must land in the slow bucket.
	for i := 0; i < 90; i++ {
		r.Observe("latency_seconds", nil, 0.005)
	}
	for i := 0; i < 10; i++ {
		r.Observe("latency_seconds", nil, 0.5)
	}
	p50 := r.Quantile("latency_seconds", nil, 0.5)
	if p50 <= 0 || p50 > 0.01 {
		t.Errorf("p50 = %v, want it inside the first bucket", p50)
	}
	p95 := r.Quantile("latency_seconds", nil, 0.95)
	if p95 <= 0.1 || p95 > 1 {
		t.Errorf("p95 = %v, want it inside the (0.1, 1] bucket", p95)
	}
	if got := r.Quantile("latency_seconds", Labels{"nope": "1"}, 0.5); got != 0 {
		t.Errorf("absent histogram quantile = %v, want 0", got)
	}
}

func TestHistogramOverflowBucket(t *testing.T) {
	r := NewRegistry()
	r.DefineHistogram("h", "", []float64{0.1})
	r.Observe("h", nil, 99)
	if got := r.Quantile("h", nil, 0.99); !math.IsInf(got, 1) {
		t.Errorf("quantile = %v, want +Inf for an observation past the last bucket", got)
	}
	var b strings.Builder
	if err := r.WriteProm(&b); err != nil {
		t.Fatalf("WriteProm: %v", err)
	}
	if !strings.Contains(b.String(), `h_bucket{le="+Inf"} 1`) {
		t.Errorf("exposition missing the +Inf bucket:\n%s", b.String())
	}
}

func TestWritePromFormat(t *testing.T) {
	r := NewRegistry()
	r.DefineCounter("payments_total", "Payments by outcome.")
	r.DefineHistogram("capture_seconds", "Capture latency.", []float64{0.01, 0.1})
	r.Inc("payments_total", Labels{"outcome": "created"})
	r.Observe("capture_seconds", Labels{"outcome": "created"}, 0.05)

	var b strings.Builder
	if err := r.WriteProm(&b); err != nil {
		t.Fatalf("WriteProm: %v", err)
	}
	out := b.String()
	for _, want := range []string{
		"# HELP payments_total Payments by outcome.",
		"# TYPE payments_total counter",
		`payments_total{outcome="created"} 1`,
		"# TYPE capture_seconds histogram",
		`capture_seconds_bucket{outcome="created",le="0.01"} 0`,
		`capture_seconds_bucket{outcome="created",le="0.1"} 1`,
		`capture_seconds_bucket{outcome="created",le="+Inf"} 1`,
		`capture_seconds_sum{outcome="created"} 0.05`,
		`capture_seconds_count{outcome="created"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q:\n%s", want, out)
		}
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	r := NewRegistry()
	r.DefineCounter("c", "")
	r.DefineHistogram("h", "", nil)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Inc("c", Labels{"worker": "shared"})
				r.Observe("h", Labels{"worker": "shared"}, float64(j)/1000)
			}
		}(i)
	}
	wg.Wait()
	if got := r.CounterValue("c", Labels{"worker": "shared"}); got != 3200 {
		t.Errorf("counter = %d, want 3200", got)
	}
}
