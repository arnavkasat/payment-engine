// Package metrics is a small counter-and-histogram registry that renders
// itself in the Prometheus text exposition format.
//
// It is hand-rolled rather than pulled in, for two reasons: the engine needs
// exactly two metric shapes, and keeping the dependency out means the latency
// numbers in the README come from code that is in this repository and can be
// read alongside them.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultBuckets covers 500µs to 8s in powers of two, which brackets both a
// cache-warm capture and a transaction that queued behind a lock.
var DefaultBuckets = []float64{
	0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8,
}

// Labels are a metric's label set.
type Labels map[string]string

// seriesKey renders a stable identifier for one labelled series.
func seriesKey(name string, l Labels) string {
	if len(l) == 0 {
		return name
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(l[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func escapeLabel(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

type counter struct {
	key string
	val atomic.Int64
}

type histogram struct {
	key     string
	mu      sync.Mutex
	buckets []float64
	counts  []uint64
	sum     float64
	total   uint64
}

func (h *histogram) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.total++
	// Cumulative buckets are produced at render time; here only the bucket
	// the observation falls into is incremented.
	i := sort.SearchFloat64s(h.buckets, v)
	if i < len(h.counts) {
		h.counts[i]++
	} else {
		h.counts[len(h.counts)-1]++ // +Inf
	}
}

// quantile estimates a quantile by linear interpolation inside the bucket the
// rank falls in. It is an estimate, as every bucketed histogram's is; exact
// percentiles come from the load generator, which keeps raw samples.
func (h *histogram) quantile(q float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	target := q * float64(h.total)
	var cumulative float64
	lower := 0.0
	for i, b := range h.buckets {
		next := cumulative + float64(h.counts[i])
		if next >= target {
			if h.counts[i] == 0 {
				return b
			}
			frac := (target - cumulative) / float64(h.counts[i])
			return lower + frac*(b-lower)
		}
		cumulative = next
		lower = b
	}
	return math.Inf(1)
}

type metricMeta struct {
	help string
	kind string
}

// Registry holds every metric the process exports.
type Registry struct {
	mu         sync.RWMutex
	meta       map[string]metricMeta
	counters   map[string]*counter
	histograms map[string]*histogram
	buckets    map[string][]float64
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		meta:       map[string]metricMeta{},
		counters:   map[string]*counter{},
		histograms: map[string]*histogram{},
		buckets:    map[string][]float64{},
	}
}

// DefineCounter declares a counter's help text. Calling it is optional but
// makes the exposition self-describing.
func (r *Registry) DefineCounter(name, help string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.meta[name] = metricMeta{help: help, kind: "counter"}
}

// DefineHistogram declares a histogram's help text and bucket boundaries,
// which must be sorted ascending. Passing nil uses DefaultBuckets.
func (r *Registry) DefineHistogram(name, help string, buckets []float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.meta[name] = metricMeta{help: help, kind: "histogram"}
	if buckets == nil {
		buckets = DefaultBuckets
	}
	r.buckets[name] = buckets
}

// Inc adds one to a counter series.
func (r *Registry) Inc(name string, labels Labels) { r.Add(name, labels, 1) }

// Add adds delta to a counter series, creating it on first use.
func (r *Registry) Add(name string, labels Labels, delta int64) {
	key := seriesKey(name, labels)
	r.mu.RLock()
	c, ok := r.counters[key]
	r.mu.RUnlock()
	if !ok {
		r.mu.Lock()
		if c, ok = r.counters[key]; !ok {
			c = &counter{key: key}
			r.counters[key] = c
			if _, defined := r.meta[name]; !defined {
				r.meta[name] = metricMeta{kind: "counter"}
			}
		}
		r.mu.Unlock()
	}
	c.val.Add(delta)
}

// Observe records a value (conventionally seconds) in a histogram series.
func (r *Registry) Observe(name string, labels Labels, v float64) {
	key := seriesKey(name, labels)
	r.mu.RLock()
	h, ok := r.histograms[key]
	r.mu.RUnlock()
	if !ok {
		r.mu.Lock()
		if h, ok = r.histograms[key]; !ok {
			buckets := r.buckets[name]
			if buckets == nil {
				buckets = DefaultBuckets
			}
			h = &histogram{key: key, buckets: buckets, counts: make([]uint64, len(buckets)+1)}
			r.histograms[key] = h
			if _, defined := r.meta[name]; !defined {
				r.meta[name] = metricMeta{kind: "histogram"}
			}
		}
		r.mu.Unlock()
	}
	h.observe(v)
}

// CounterValue returns a counter series' current value, for tests.
func (r *Registry) CounterValue(name string, labels Labels) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if c, ok := r.counters[seriesKey(name, labels)]; ok {
		return c.val.Load()
	}
	return 0
}

// Quantile returns an estimated quantile of a histogram series.
func (r *Registry) Quantile(name string, labels Labels, q float64) float64 {
	r.mu.RLock()
	h, ok := r.histograms[seriesKey(name, labels)]
	r.mu.RUnlock()
	if !ok {
		return 0
	}
	return h.quantile(q)
}

// WriteProm renders every metric in the Prometheus text exposition format.
func (r *Registry) WriteProm(w io.Writer) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.meta))
	for name := range r.meta {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		meta := r.meta[name]
		if meta.help != "" {
			fmt.Fprintf(&b, "# HELP %s %s\n", name, meta.help)
		}
		fmt.Fprintf(&b, "# TYPE %s %s\n", name, meta.kind)

		switch meta.kind {
		case "counter":
			for _, key := range sortedKeys(r.counters, name) {
				fmt.Fprintf(&b, "%s %d\n", key, r.counters[key].val.Load())
			}
		case "histogram":
			for _, key := range sortedKeys(r.histograms, name) {
				writeHistogram(&b, name, key, r.histograms[key])
			}
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func writeHistogram(b *strings.Builder, name, key string, h *histogram) {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, labels := splitKey(key)
	var cumulative uint64
	for i, bound := range h.buckets {
		cumulative += h.counts[i]
		fmt.Fprintf(b, "%s_bucket%s %d\n", name, withLabel(labels, "le", strconv.FormatFloat(bound, 'g', -1, 64)), cumulative)
	}
	cumulative += h.counts[len(h.counts)-1]
	fmt.Fprintf(b, "%s_bucket%s %d\n", name, withLabel(labels, "le", "+Inf"), cumulative)
	fmt.Fprintf(b, "%s_sum%s %g\n", name, labels, h.sum)
	fmt.Fprintf(b, "%s_count%s %d\n", name, labels, h.total)
}

// splitKey separates a series key into its metric name and its rendered
// label block, which may be empty.
func splitKey(key string) (string, string) {
	i := strings.IndexByte(key, '{')
	if i < 0 {
		return key, ""
	}
	return key[:i], key[i:]
}

// withLabel splices an extra label into a rendered label block.
func withLabel(labels, name, value string) string {
	pair := name + `="` + escapeLabel(value) + `"`
	if labels == "" {
		return "{" + pair + "}"
	}
	return labels[:len(labels)-1] + "," + pair + "}"
}

func sortedKeys[T any](m map[string]T, name string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		if base, _ := splitKey(k); base == name {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
